

#include"checksum.hpp"
#include"protocol.hpp"

#include<arpa/inet.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<poll.h>
#include<sys/socket.h>
#include<sys/stat.h>
#include<unistd.h>

#include<algorithm>
#include<array>
#include<cerrno>
#include<chrono>
#include<cstdint>
#include<cstring>
#include<iostream>
#include<limits>
#include<optional>
#include<random>
#include<stdexcept>
#include<string>
#include<thread>
#include<unordered_set>
#include<utility>
#include<vector>

namespace
{
class FileDescriptor
{
public:
    explicit FileDescriptor(int fd=-1):fd_(fd) {}
    ~FileDescriptor()
    {
        if(fd_>=0)
        {
            ::close(fd_);
        }
    }

    FileDescriptor(const FileDescriptor&)=delete;
    FileDescriptor&operator=(const FileDescriptor&)=delete;

    FileDescriptor(FileDescriptor&&other)noexcept : fd_(other.fd_)
    {
        other.fd_=-1;
    }

    FileDescriptor&operator=(FileDescriptor&&other)noexcept {

        if(this!=&other)
        {
            if(fd_>=0)
            {
                ::close(fd_);
            }
            fd_=other.fd_;
            other.fd_=-1;
        }
        return *this;
    }

    int get()const { return fd_; }

private:
    int fd_;
};

[[noreturn]] void system_error(const std::string&operation)
{
    throw std::runtime_error(operation+": "+std::strerror(errno));
}

using SteadyClock=std::chrono::steady_clock;
constexpr auto kReportTimeout=std::chrono::seconds(30);
constexpr auto kMetaReadyTimeout=std::chrono::seconds(30);
constexpr auto kReadyTimeout=std::chrono::seconds(10);
constexpr std::uint32_t kSectionId=srcast::kSingleSectionId;
constexpr std::size_t kMulticastRepairThreshold=2;

std::uint64_t create_transfer_id()
{
    std::random_device device;
    std::mt19937_64 engine(device());
    return engine();
}

void write_all(int fd,const std::uint8_t*data,std::size_t size)
{
    std::size_t written=0;
    while(written<size)
    {
        const auto count=::send(
            fd,
            data+written,
            size-written,
            MSG_NOSIGNAL);
        if(count<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("send control frame");
        }
        if(count==0)
        {
            throw std::runtime_error("control connection made no write progress");
        }
        written+=static_cast<std::size_t>(count);
    }
}

void read_all(int fd,std::uint8_t*data,std::size_t size)
{
    std::size_t received=0;
    while(received<size)
    {
        const auto count=::recv(fd,data+received,size-received,0);
        if(count<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("recv control frame");
        }
        if(count==0)
        {
            throw std::runtime_error("control connection closed");
        }
        received+=static_cast<std::size_t>(count);
    }
}

void send_control_frame(int fd,const std::vector<std::uint8_t>&frame)
{
    if(frame.empty()||frame.size()>srcast::kMaxControlFrameSize)
    {
        throw std::runtime_error("invalid control frame size");
    }
    const auto network_size=htonl(static_cast<std::uint32_t>(frame.size()));
    write_all(
        fd,
        reinterpret_cast<const std::uint8_t*>(&network_size),
        sizeof(network_size));
    write_all(fd,frame.data(),frame.size());
}

std::vector<std::uint8_t>receive_control_frame(int fd)
{
    std::uint32_t network_size{};
    read_all(
        fd,
        reinterpret_cast<std::uint8_t*>(&network_size),
        sizeof(network_size));
    const auto size=ntohl(network_size);
    if(size==0||size>srcast::kMaxControlFrameSize)
    {
        throw std::runtime_error("invalid received control frame size");
    }
    std::vector<std::uint8_t>frame(size);
    read_all(fd,frame.data(),frame.size());
    return frame;
}

void send_udp_packet(
    int socket_fd,
    const sockaddr_in&destination,
    const std::vector<std::uint8_t>&packet)
    {

    const auto sent=::sendto(
        socket_fd,
        packet.data(),
        packet.size(),
        0,
        reinterpret_cast<const sockaddr*>(&destination),
        sizeof(destination));

    if(sent<0)
    {
        system_error("sendto");
    }
    if(static_cast<std::size_t>(sent)!=packet.size())
    {
        throw std::runtime_error("sendto sent a partial datagram");
    }
}

void read_block(
    int input_fd,
    const std::string&file_path,
    std::uint64_t file_size,
    std::uint32_t block_id,
    std::array<std::uint8_t,srcast::kPayloadSize>&buffer,
    std::uint64_t&offset,
    std::uint16_t&payload_size)
    {

    offset=static_cast<std::uint64_t>(block_id) * srcast::kPayloadSize;
    const auto wanted=static_cast<std::size_t>(
        std::min<std::uint64_t>(srcast::kPayloadSize,file_size-offset));

    std::size_t received=0;
    while(received<wanted)
    {
        const auto count=::pread(
            input_fd,
            buffer.data()+received,
            wanted-received,
            static_cast<off_t>(offset+received));
        if(count<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("pread "+file_path);
        }
        if(count==0)
        {
            throw std::runtime_error(
                "unexpected EOF while reading source file: "+file_path);
        }
        received+=static_cast<std::size_t>(count);
    }

    payload_size=static_cast<std::uint16_t>(wanted);
}

struct ReceiverConnection
{
    std::uint64_t receiver_id{};
    FileDescriptor control_fd;
    sockaddr_in udp_destination{};

    bool meta_ready{false};

    bool status_received{false};
    bool complete_received{false};
    bool completed{false};
    bool ready_for_next_transfer{false};

    std::optional<srcast::SectionStatusMessage>status;
};

void usage(const char*program)
{
    std::cerr
<<"Usage: "<<program
<<"<multicast_ip><udp_port><interface_ip><control_port>"
<<"<receiver_count><pace_us><gap_ms><max_repair_rounds>"
<<"<file1>[file2 ...]\n"
<<"Example: "<<program
<<" 239.255.42.99 5000 127.0.0.1 6000 3 200 1500 3"
<<" input-a.bin input-b.bin\n";
}

FileDescriptor create_control_listener(int control_port)
{
    FileDescriptor listener(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0));
    if(listener.get()<0)
    {
        system_error("socket TCP listener");
    }

    const int reuse=1;
    if(::setsockopt(
            listener.get(),
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            sizeof(reuse))!=0)
            {
        system_error("setsockopt TCP SO_REUSEADDR");
    }

    sockaddr_in local{};
    local.sin_family=AF_INET;
    local.sin_port=htons(static_cast<std::uint16_t>(control_port));
    local.sin_addr.s_addr=htonl(INADDR_ANY);

    if(::bind(
            listener.get(),
            reinterpret_cast<sockaddr*>(&local),
            sizeof(local))!=0)
            {
        system_error("bind TCP listener");
    }
    if(::listen(listener.get(),64)!=0)
    {
        system_error("listen TCP listener");
    }
    return listener;
}

std::vector<ReceiverConnection>accept_receivers(
    int listener_fd,
    std::size_t expected_receivers)
    {

    std::vector<ReceiverConnection>receivers;
    receivers.reserve(expected_receivers);
    std::unordered_set<std::uint64_t>receiver_ids;

    std::cout<<"waiting for "<<expected_receivers
<<" receiver control connections\n";

    while(receivers.size()<expected_receivers)
    {
        sockaddr_in peer{};
        socklen_t peer_size=sizeof(peer);
        const int accepted=::accept4(
            listener_fd,
            reinterpret_cast<sockaddr*>(&peer),
            &peer_size,
            SOCK_CLOEXEC);
        if(accepted<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("accept receiver");
        }

        try
        {
            FileDescriptor control_fd(accepted);
            const auto frame=receive_control_frame(control_fd.get());
            const auto registration=srcast::decode_register(frame);

            if(registration.receiver_id==0)
            {
                throw std::runtime_error("receiver_id must not be zero");
            }
            if(registration.udp_port==0)
            {
                throw std::runtime_error("receiver UDP port must not be zero");
            }
            if(!receiver_ids.insert(registration.receiver_id).second)
            {
                throw std::runtime_error("duplicate receiver_id");
            }

            ReceiverConnection receiver;
            receiver.receiver_id=registration.receiver_id;

            receiver.control_fd=std::move(control_fd);
            receiver.udp_destination=peer;
            receiver.udp_destination.sin_port=htons(registration.udp_port);

            char address[INET_ADDRSTRLEN]{};
            if(::inet_ntop(
                    AF_INET,
                    &peer.sin_addr,
                    address,
                    sizeof(address))==nullptr)
                    {
                std::strcpy(address,"?");
            }

            std::cout<<"registered receiver_id="<<receiver.receiver_id
<<" address="<<address<<':'
<<registration.udp_port<<'\n';
            receivers.push_back(std::move(receiver));
        } catch(...) {
            throw;
        }
    }

    return receivers;
}

bool validate_missing_bitmap(
    const srcast::SectionStatusMessage&status,
    std::uint32_t total_blocks)
    {

    const auto expected_size=srcast::bitmap_size_for_blocks(total_blocks);
    if(status.status==srcast::SectionStatusCode::Missing)
    {
        if(status.missing_bitmap.size()!=expected_size||
            status.missing_count==0)
            {
            return false;
        }

        std::uint32_t counted=0;
        for(std::uint32_t block_id=0; block_id<total_blocks;++block_id)
        {
            if(srcast::bitmap_test(status.missing_bitmap,block_id))
            {
++counted;
            }
        }

        if(total_blocks%8U!=0U &&!status.missing_bitmap.empty())
        {
            const auto used_bits=static_cast<unsigned int>(total_blocks%8U);
            const auto unused_mask=static_cast<std::uint8_t>(
                0xffU<<used_bits);
            if((status.missing_bitmap.back() & unused_mask)!=0U)
            {
                return false;
            }
        }
        return counted==status.missing_count;
    }

    return status.missing_count==0 && status.missing_bitmap.empty();
}

void reset_round_state(std::vector<ReceiverConnection>&receivers)
{
    for(auto&receiver : receivers)
    {
        receiver.status_received=false;
        receiver.complete_received=false;
        receiver.ready_for_next_transfer=false;
        receiver.status.reset();
    }
}

bool round_responses_complete(
    const std::vector<ReceiverConnection>&receivers)
    {

    for(const auto&receiver : receivers)
    {
        if(receiver.completed)
        {
            continue;
        }
        if(!receiver.status_received)
        {
            return false;
        }
        if(receiver.status &&
            receiver.status->status==srcast::SectionStatusCode::Complete &&
!receiver.complete_received)
            {
            return false;
        }
    }
    return true;
}

void handle_control_frame(
    ReceiverConnection&receiver,
    const std::vector<std::uint8_t>&frame,
    std::uint64_t transfer_id,
    std::uint32_t round_id,
    std::uint64_t file_size,
    std::uint32_t total_blocks,
    const std::array<std::uint8_t,srcast::kSha256Size>&digest)
    {

    const auto type=srcast::peek_control_type(frame);

    if(type==srcast::ControlType::SectionStatus)
    {
        const auto status=srcast::decode_section_status(frame);
        if(status.receiver_id!=receiver.receiver_id||
            status.transfer_id!=transfer_id||
            status.section_id!=kSectionId||
            status.round_id!=round_id)
            {
            std::cerr<<"ignore stale or mismatched SECTION_STATUS from receiver_id="
<<receiver.receiver_id<<'\n';
            return;
        }
        if(!validate_missing_bitmap(status,total_blocks))
        {
            std::cerr<<"reject invalid missing bitmap from receiver_id="
<<receiver.receiver_id<<'\n';
            return;
        }

        receiver.status=status;
        receiver.status_received=true;

        std::cout<<"round="<<round_id
<<" receiver_id="<<receiver.receiver_id
<<" status=";
        if(status.status==srcast::SectionStatusCode::Complete)
        {
            std::cout<<"COMPLETE";
        } else if(status.status==srcast::SectionStatusCode::Missing) {
            std::cout<<"MISSING missing_blocks="<<status.missing_count;
        }
        else
        {
            std::cout<<"FAILED";
        }
        std::cout<<'\n';
        return;
    }

    if(type==srcast::ControlType::ReceiverComplete)
    {
        const auto complete=srcast::decode_receiver_complete(frame);
        if(!receiver.status_received||!receiver.status||
            receiver.status->status!=srcast::SectionStatusCode::Complete||
            complete.receiver_id!=receiver.receiver_id||
            complete.transfer_id!=transfer_id||
            complete.file_size!=file_size||
            complete.sha256!=digest)
            {
            std::cerr<<"reject invalid RECEIVER_COMPLETE from receiver_id="
<<receiver.receiver_id<<'\n';
            return;
        }

        receiver.complete_received=true;
        receiver.completed=true;
        send_control_frame(
            receiver.control_fd.get(),
            srcast::encode_complete_ack(
                {receiver.receiver_id,transfer_id}));
        std::cout<<"completion acknowledged receiver_id="
<<receiver.receiver_id<<'\n';
        return;
    }

    std::cerr<<"ignore unexpected control message from receiver_id="
<<receiver.receiver_id<<'\n';
}

void collect_round_reports(
    std::vector<ReceiverConnection>&receivers,
    std::uint64_t transfer_id,
    std::uint32_t round_id,
    std::uint64_t file_size,
    std::uint32_t total_blocks,
    const std::array<std::uint8_t,srcast::kSha256Size>&digest)
    {

    reset_round_state(receivers);
    const auto deadline=SteadyClock::now()+kReportTimeout;

    while(!round_responses_complete(receivers))
    {
        const auto now=SteadyClock::now();
        if(now>=deadline)
        {
            break;
        }

        const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline-now);
        const auto timeout_ms=static_cast<int>(
            std::max<std::int64_t>(1,remaining.count()));

        std::vector<pollfd>poll_fds;
        std::vector<std::size_t>indexes;
        poll_fds.reserve(receivers.size());
        indexes.reserve(receivers.size());

        for(std::size_t index=0; index<receivers.size();++index)
        {
            if(receivers[index].completed)
            {
                continue;
            }
            poll_fds.push_back({receivers[index].control_fd.get(),POLLIN,0});
            indexes.push_back(index);
        }

        if(poll_fds.empty())
        {
            break;
        }

        const int ready=::poll(
            poll_fds.data(),
            static_cast<nfds_t>(poll_fds.size()),
            timeout_ms);
        if(ready<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("poll receiver reports");
        }
        if(ready==0)
        {
            break;
        }

        for(std::size_t poll_index=0;
             poll_index<poll_fds.size();
++poll_index)
             {
            const auto events=poll_fds[poll_index].revents;
            if((events & (POLLERR|POLLHUP|POLLNVAL))!=0)
            {
                throw std::runtime_error("receiver control connection failed");
            }
            if((events & POLLIN)==0)
            {
                continue;
            }

            auto&receiver=receivers[indexes[poll_index]];
            const auto frame=receive_control_frame(receiver.control_fd.get());
            handle_control_frame(
                receiver,
                frame,
                transfer_id,
                round_id,
                file_size,
                total_blocks,
                digest);
        }
    }

    for(const auto&receiver : receivers)
    {
        if(!receiver.completed &&!receiver.status_received)
        {
            std::cout<<"round="<<round_id
<<" receiver_id="<<receiver.receiver_id
<<" status=NO_REPORT\n";
        }
    }
}

std::vector<std::vector<std::size_t>>aggregate_missing_blocks(
    const std::vector<ReceiverConnection>&receivers,
    std::uint32_t total_blocks)
    {

    std::vector<std::vector<std::size_t>>missing_by_block(total_blocks);

    for(std::size_t receiver_index=0;
         receiver_index<receivers.size();
++receiver_index)
         {
        const auto&receiver=receivers[receiver_index];
        if(receiver.completed||!receiver.status||
            receiver.status->status!=srcast::SectionStatusCode::Missing)
            {
            continue;
        }

        for(std::uint32_t block_id=0;
             block_id<total_blocks;
++block_id)
             {
            if(srcast::bitmap_test(
                    receiver.status->missing_bitmap,
                    block_id))
                    {
                missing_by_block[block_id].push_back(receiver_index);
            }
        }
    }

    return missing_by_block;
}

void send_end_round(
    int udp_fd,
    const sockaddr_in&multicast_destination,
    std::vector<ReceiverConnection>&receivers,
    std::uint64_t transfer_id,
    std::uint32_t round_id,
    std::uint32_t total_blocks)
    {

    const auto end_packet=srcast::encode_end(
        transfer_id,
        kSectionId,
        round_id,
        total_blocks);

    for(int repetition=0; repetition<10;++repetition)
    {
        send_udp_packet(udp_fd,multicast_destination,end_packet);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    const auto section_end=srcast::encode_section_end(
        {transfer_id,kSectionId,round_id,total_blocks});
    for(auto&receiver : receivers)
    {
        if(!receiver.completed)
        {
            send_control_frame(receiver.control_fd.get(),section_end);
        }
    }
}

void send_repair_round(
    int udp_fd,
    const sockaddr_in&multicast_destination,
    std::vector<ReceiverConnection>&receivers,
    int input_fd,
    const std::string&file_path,
    std::uint64_t file_size,
    std::uint64_t transfer_id,
    std::uint32_t total_blocks,
    std::uint32_t round_id,
    int pace_us)
    {

    const auto repair_begin=srcast::encode_repair_begin(
        {transfer_id,kSectionId,round_id});
    for(auto&receiver : receivers)
    {
        if(!receiver.completed)
        {
            send_control_frame(receiver.control_fd.get(),repair_begin);
        }
    }

    const auto missing_by_block=aggregate_missing_blocks(
        receivers,
        total_blocks);

    std::array<std::uint8_t,srcast::kPayloadSize>buffer{};
    std::uint64_t multicast_packets=0;
    std::uint64_t unicast_packets=0;

    for(std::uint32_t block_id=0;
         block_id<total_blocks;
++block_id)
         {
        const auto&targets=missing_by_block[block_id];
        if(targets.empty())
        {
            continue;
        }

        std::uint64_t offset{};
        std::uint16_t payload_size{};
        read_block(
            input_fd,
            file_path,
            file_size,
            block_id,
            buffer,
            offset,
            payload_size);

        const auto packet=srcast::encode_data(
            transfer_id,
            kSectionId,
            block_id,
            offset,
            buffer.data(),
            payload_size,
            srcast::crc32(buffer.data(),payload_size));

        if(targets.size()>=kMulticastRepairThreshold)
        {
            send_udp_packet(udp_fd,multicast_destination,packet);
++multicast_packets;
        }
        else
        {
            const auto receiver_index=targets.front();
            send_udp_packet(
                udp_fd,
                receivers[receiver_index].udp_destination,
                packet);
++unicast_packets;
        }

        if(pace_us>0)
        {
            std::this_thread::sleep_for(
                std::chrono::microseconds(pace_us));
        }
    }

    std::cout<<"repair round="<<round_id
<<" multicast_packets="<<multicast_packets
<<" unicast_packets="<<unicast_packets<<'\n';

    send_end_round(
        udp_fd,
        multicast_destination,
        receivers,
        transfer_id,
        round_id,
        total_blocks);
}

bool all_receivers_completed(
    const std::vector<ReceiverConnection>&receivers)
    {

    return std::all_of(
        receivers.begin(),
        receivers.end(),
        [](const ReceiverConnection&receiver)
        {
            return receiver.completed;
        });
}

void wait_for_receivers_ready(
    std::vector<ReceiverConnection>&receivers,
    std::uint64_t transfer_id)
    {

    for(auto&receiver : receivers)
    {
        receiver.ready_for_next_transfer=false;
    }

    const auto deadline=SteadyClock::now()+kReadyTimeout;
    for(;;)
    {
        const bool all_ready=std::all_of(
            receivers.begin(),
            receivers.end(),
            [](const ReceiverConnection&receiver)
            {
                return receiver.ready_for_next_transfer;
            });
        if(all_ready)
        {
            std::cout<<"all receivers are ready for the next transfer\n";
            return;
        }

        const auto now=SteadyClock::now();
        if(now>=deadline)
        {
            throw std::runtime_error(
                "timed out waiting for receivers to reset transfer state");
        }

        const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline-now);
        const auto timeout_ms=static_cast<int>(
            std::max<std::int64_t>(1,remaining.count()));

        std::vector<pollfd>poll_fds;
        std::vector<std::size_t>indexes;
        for(std::size_t index=0; index<receivers.size();++index)
        {
            if(!receivers[index].ready_for_next_transfer)
            {
                poll_fds.push_back(
                    {receivers[index].control_fd.get(),POLLIN,0});
                indexes.push_back(index);
            }
        }

        const int ready=::poll(
            poll_fds.data(),
            static_cast<nfds_t>(poll_fds.size()),
            timeout_ms);
        if(ready<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            system_error("poll receiver ready acknowledgements");
        }
        if(ready==0)
        {
            continue;
        }

        for(std::size_t poll_index=0;
             poll_index<poll_fds.size();
++poll_index)
             {
            const auto events=poll_fds[poll_index].revents;
            if((events & (POLLERR|POLLHUP|POLLNVAL))!=0)
            {
                throw std::runtime_error("receiver control connection failed");
            }
            if((events & POLLIN)==0)
            {
                continue;
            }

            auto&receiver=receivers[indexes[poll_index]];
            const auto frame=receive_control_frame(receiver.control_fd.get());
            if(srcast::peek_control_type(frame)!=
                srcast::ControlType::ReceiverReady)
                {
                std::cerr<<"ignore unexpected message while waiting for ready"
<<" receiver_id="<<receiver.receiver_id<<'\n';
                continue;
            }

            const auto message=srcast::decode_receiver_ready(frame);
            if(message.receiver_id!=receiver.receiver_id||
                message.previous_transfer_id!=transfer_id)
                {
                std::cerr<<"ignore stale RECEIVER_READY receiver_id="
<<receiver.receiver_id<<'\n';
                continue;
            }

            receiver.ready_for_next_transfer=true;
            std::cout<<"receiver ready receiver_id="
<<receiver.receiver_id<<'\n';
        }
    }
}

void notify_failed_receivers(
    std::vector<ReceiverConnection>&receivers,
    std::uint64_t transfer_id)
    {

    const auto frame=srcast::encode_transfer_result(
        {transfer_id,srcast::TransferResultCode::Failed});
    for(auto&receiver : receivers)
    {
        if(!receiver.completed)
        {
            send_control_frame(receiver.control_fd.get(),frame);
        }
    }
}

void wait_for_meta_ready(
    std::vector<ReceiverConnection>&receivers,
    std::uint64_t transfer_id)
    {

    const auto deadline=
        SteadyClock::now()+kMetaReadyTimeout;

    for(;;)
    {
        const bool all_ready=std::all_of(
            receivers.begin(),
            receivers.end(),
            [](const ReceiverConnection&receiver)
            {
                return receiver.meta_ready;
            });

        if(all_ready)
        {
            std::cout
<<"all receivers accepted FILE_META; "
<<"starting UDP DATA\n";
            return;
        }

        const auto now=SteadyClock::now();

        if(now>=deadline)
        {
            throw std::runtime_error(
                "timed out waiting for META_READY");
        }

        const auto remaining=
            std::chrono::duration_cast<
                std::chrono::milliseconds>(
                    deadline-now);

        const auto timeout_ms=
            static_cast<int>(
                std::max<std::int64_t>(
                    1,
                    remaining.count()));

        std::vector<pollfd>poll_fds;
        std::vector<std::size_t>indexes;

        poll_fds.reserve(receivers.size());
        indexes.reserve(receivers.size());

        for(std::size_t index=0;
             index<receivers.size();
++index)
             {

            if(receivers[index].meta_ready)
            {
                continue;
            }

            pollfd item{};
            item.fd=
                receivers[index].control_fd.get();
            item.events=POLLIN;

            poll_fds.push_back(item);
            indexes.push_back(index);
        }

        const int ready=::poll(
            poll_fds.data(),
            static_cast<nfds_t>(
                poll_fds.size()),
            timeout_ms);

        if(ready<0)
        {
            if(errno==EINTR)
            {
                continue;
            }

            system_error("poll META_READY");
        }

        if(ready==0)
        {
            continue;
        }

        for(std::size_t poll_index=0;
             poll_index<poll_fds.size();
++poll_index)
             {

            const auto events=
                poll_fds[poll_index].revents;

            if((events &
                 (POLLERR|
                  POLLHUP|
                  POLLNVAL))!=0)
                  {

                throw std::runtime_error(
                    "receiver control connection "
                    "failed while waiting for "
                    "META_READY");
            }

            if((events & POLLIN)==0)
            {
                continue;
            }

            auto&receiver=
                receivers[indexes[poll_index]];

            const auto frame=
                receive_control_frame(
                    receiver.control_fd.get());

            const auto type=
                srcast::peek_control_type(frame);

            if(type!=
                srcast::ControlType::MetaReady)
                {

                throw std::runtime_error(
                    "unexpected control message "
                    "while waiting for META_READY");
            }

            const auto message=
                srcast::decode_meta_ready(frame);

            if(message.receiver_id!=
                    receiver.receiver_id||
                message.transfer_id!=
                    transfer_id)
                    {

                throw std::runtime_error(
                    "invalid or stale META_READY");
            }

            receiver.meta_ready=true;

            std::cout
<<"META_READY receiver_id="
<<receiver.receiver_id
<<'\n';
        }
    }
}

void send_file(
    int udp_fd,
    const sockaddr_in&multicast_destination,
    std::vector<ReceiverConnection>&receivers,
    const std::string&file_path,
    int pace_us,
    int max_repair_rounds,
    std::size_t file_index,
    std::size_t file_count)
    {

    for(auto&receiver : receivers)
    {
        receiver.meta_ready=false;
        receiver.completed=false;
        receiver.status_received=false;
        receiver.complete_received=false;
        receiver.ready_for_next_transfer=false;
        receiver.status.reset();
    }

    struct stat status{};
    if(::stat(file_path.c_str(),&status)!=0)
    {
        system_error("stat "+file_path);
    }
    if(!S_ISREG(status.st_mode)||status.st_size<0)
    {
        throw std::runtime_error(
            "source path must be a regular file: "+file_path);
    }

    const auto file_size=static_cast<std::uint64_t>(status.st_size);
    const auto total_blocks64=
        (file_size+srcast::kPayloadSize-1)/srcast::kPayloadSize;

    if(total_blocks64>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error(
            "file is too large for protocol block numbering: "+file_path);
    }

    const auto total_blocks=static_cast<std::uint32_t>(total_blocks64);
    const auto digest=srcast::sha256_file(file_path);
    const auto transfer_id=create_transfer_id();

    FileDescriptor input(::open(file_path.c_str(),O_RDONLY|O_CLOEXEC));
    if(input.get()<0)
    {
        system_error("open source file "+file_path);
    }

    srcast::FileMetaMessage file_meta;

    file_meta.transfer_id=transfer_id;
    file_meta.section_id=kSectionId;
    file_meta.file_size=file_size;
    file_meta.block_size=srcast::kPayloadSize;
    file_meta.total_blocks=total_blocks;
    file_meta.sha256=digest;

    const auto file_meta_frame=
        srcast::encode_file_meta(file_meta);

    for(auto&receiver : receivers)
    {
        send_control_frame(
            receiver.control_fd.get(),
            file_meta_frame);
    }

    wait_for_meta_ready(
        receivers,
        transfer_id);

    std::array<std::uint8_t,srcast::kPayloadSize>buffer{};

    for(std::uint32_t block_id=0;
         block_id<total_blocks;
++block_id)
         {
        std::uint64_t offset{};
        std::uint16_t payload_size{};
        read_block(
            input.get(),
            file_path,
            file_size,
            block_id,
            buffer,
            offset,
            payload_size);

        const auto packet=srcast::encode_data(
            transfer_id,
            kSectionId,
            block_id,
            offset,
            buffer.data(),
            payload_size,
            srcast::crc32(buffer.data(),payload_size));

        send_udp_packet(udp_fd,multicast_destination,packet);

        if((block_id+1U)%1000U==0U)
        {
            send_udp_packet(udp_fd,multicast_destination,meta_packet);
            std::cout<<"sent "<<(block_id+1U)<<'/'
<<total_blocks<<" blocks\n";
        }

        if(pace_us>0)
        {
            std::this_thread::sleep_for(
                std::chrono::microseconds(pace_us));
        }
    }

    send_end_round(
        udp_fd,
        multicast_destination,
        receivers,
        transfer_id,
        0,
        total_blocks);

    std::cout<<"initial multicast finished; collecting round 0 reports\n";
    collect_round_reports(
        receivers,
        transfer_id,
        0,
        file_size,
        total_blocks,
        digest);

    for(int repair_round=1;
         repair_round<=max_repair_rounds &&
!all_receivers_completed(receivers);
++repair_round)
         {

        send_repair_round(
            udp_fd,
            multicast_destination,
            receivers,
            input.get(),
            file_path,
            file_size,
            transfer_id,
            total_blocks,
            static_cast<std::uint32_t>(repair_round),
            pace_us);

        collect_round_reports(
            receivers,
            transfer_id,
            static_cast<std::uint32_t>(repair_round),
            file_size,
            total_blocks,
            digest);
    }

    if(all_receivers_completed(receivers))
    {
        std::cout<<"file completed by all receivers: "
<<file_path<<'\n';
    }
    else
    {
        notify_failed_receivers(receivers,transfer_id);
        const auto completed_count=static_cast<std::size_t>(std::count_if(
            receivers.begin(),
            receivers.end(),
            [](const ReceiverConnection&receiver)
            {
                return receiver.completed;
            }));
        std::cout<<"file finished PARTIAL completed="
<<completed_count<<'/'<<receivers.size()
<<" after max repair rounds\n";
    }

    wait_for_receivers_ready(receivers,transfer_id);
}

}

int main(int argc,char** argv) try {
    if(argc<10)
    {
        usage(argv[0]);
        return 2;
    }

    const std::string multicast_ip=argv[1];
    const int udp_port=std::stoi(argv[2]);
    const std::string interface_ip=argv[3];
    const int control_port=std::stoi(argv[4]);
    const int receiver_count=std::stoi(argv[5]);
    const int pace_us=std::stoi(argv[6]);
    const int gap_ms=std::stoi(argv[7]);
    const int max_repair_rounds=std::stoi(argv[8]);

    if(udp_port<1||udp_port>65535||
        control_port<1||control_port>65535||
        receiver_count<1||
        pace_us<0||gap_ms<0||max_repair_rounds<0)
        {
        throw std::runtime_error("invalid command-line argument");
    }

    FileDescriptor udp_fd(::socket(
        AF_INET,
        SOCK_DGRAM|SOCK_CLOEXEC,
        0));
    if(udp_fd.get()<0)
    {
        system_error("socket UDP");
    }

    const unsigned char ttl=1;
    const unsigned char loop=1;
    if(::setsockopt(
            udp_fd.get(),
            IPPROTO_IP,
            IP_MULTICAST_TTL,
            &ttl,
            sizeof(ttl))!=0)
            {
        system_error("setsockopt IP_MULTICAST_TTL");
    }
    if(::setsockopt(
            udp_fd.get(),
            IPPROTO_IP,
            IP_MULTICAST_LOOP,
            &loop,
            sizeof(loop))!=0)
            {
        system_error("setsockopt IP_MULTICAST_LOOP");
    }

    if(interface_ip!="0.0.0.0")
    {
        in_addr interface_address{};
        if(::inet_pton(
                AF_INET,
                interface_ip.c_str(),
                &interface_address)!=1)
                {
            throw std::runtime_error("invalid interface IPv4 address");
        }
        if(::setsockopt(
                udp_fd.get(),
                IPPROTO_IP,
                IP_MULTICAST_IF,
                &interface_address,
                sizeof(interface_address))!=0)
                {
            system_error("setsockopt IP_MULTICAST_IF");
        }
    }

    sockaddr_in multicast_destination{};
    multicast_destination.sin_family=AF_INET;
    multicast_destination.sin_port=htons(
        static_cast<std::uint16_t>(udp_port));
    if(::inet_pton(
            AF_INET,
            multicast_ip.c_str(),
            &multicast_destination.sin_addr)!=1||
!IN_MULTICAST(ntohl(multicast_destination.sin_addr.s_addr)))
        {
        throw std::runtime_error(
            "destination must be an IPv4 multicast address");
    }

    auto listener=create_control_listener(control_port);
    auto receivers=accept_receivers(
        listener.get(),
        static_cast<std::size_t>(receiver_count));

    const std::size_t file_count=static_cast<std::size_t>(argc-9);

    for(int argument_index=9;
         argument_index<argc;
++argument_index)
         {
        const std::size_t file_index=
            static_cast<std::size_t>(argument_index-8);

        send_file(
            udp_fd.get(),
            multicast_destination,
            receivers,
            argv[argument_index],
            pace_us,
            max_repair_rounds,
            file_index,
            file_count);

        if(argument_index+1<argc && gap_ms>0)
        {
            std::cout<<"waiting "<<gap_ms
<<"ms before the next file\n";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(gap_ms));
        }
    }

    const auto session_end=srcast::encode_session_end();
    for(auto&receiver : receivers)
    {
        send_control_frame(receiver.control_fd.get(),session_end);
    }

    std::cout<<"all files processed serially\n";
    return 0;

} catch(const std::exception&error) {
    std::cerr<<"sender error: "<<error.what()<<'\n';
    return 1;
}