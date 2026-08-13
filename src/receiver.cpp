#include"checksum.hpp"
#include"control_io.hpp"
#include"file_descriptor.hpp"
#include"file_io.hpp"
#include"protocol.hpp"
#include"storage_utils.hpp"
#include"transfer_options.hpp"

#include<arpa/inet.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<sys/epoll.h>
#include<sys/socket.h>
#include<sys/stat.h>
#include<sys/statvfs.h>
#include<sys/timerfd.h>
#include<unistd.h>
#include<algorithm>
#include<array>
#include<cerrno>
#include<chrono>
#include<cstdint>
#include<cstdlib>
#include<cstring>
#include<filesystem>
#include<fstream>
#include<iostream>
#include<limits>
#include<optional>
#include<sstream>
#include<stdexcept>
#include<string>
#include<unordered_set>
#include<vector>

namespace
{
using SteadyClock=std::chrono::steady_clock;
constexpr auto kDrainQuietPeriod=std::chrono::milliseconds(200);//等待UDP数据安静的时间
constexpr auto kDrainHardTimeout=std::chrono::seconds(1);//等待缺块统计的最长时间
constexpr std::size_t kMaxUdpPacketsPerBatch=256;//单轮最多处理的UDP包数量
constexpr auto kUdpBatchTimeBudget=std::chrono::milliseconds(2);//单轮处理UDP的最长时间

class ControlStreamReader{
public:
    std::vector<std::vector<std::uint8_t>>read_frames(int fd)
    {
        for(;;)
        {
            std::array<std::uint8_t,4096>chunk{};
            const auto count=::recv(fd,chunk.data(),chunk.size(),MSG_DONTWAIT);
            if(count<0)
            {
                if(errno==EINTR)continue;
                if(errno==EAGAIN||errno==EWOULDBLOCK)break;
                system_error("recv control stream");
            }
            if(count==0){peer_closed_=true;break;}
            buffer_.insert(buffer_.end(),chunk.begin(),chunk.begin()+count);
        }

        std::vector<std::vector<std::uint8_t>>frames;
        std::size_t consumed=0;
        while(buffer_.size()-consumed>=sizeof(std::uint32_t))
        {
            std::uint32_t network_size{};
            std::memcpy(&network_size,buffer_.data()+consumed,sizeof(network_size));
            const auto frame_size=ntohl(network_size);
            if(frame_size==0||frame_size>srcast::kMaxControlFrameSize)
            {
                throw std::runtime_error("invalid control frame length");
            }

            const auto complete_size=sizeof(std::uint32_t)+static_cast<std::size_t>(frame_size);
            if(buffer_.size()-consumed<complete_size)break;

            const auto frame_begin=buffer_.begin()+static_cast<std::ptrdiff_t>
                                  (consumed+sizeof(std::uint32_t));
            frames.emplace_back(frame_begin,frame_begin+static_cast<std::ptrdiff_t>(frame_size));
            consumed+=complete_size;
        }

        if(consumed!=0)
        buffer_.erase(buffer_.begin(),buffer_.begin()+static_cast<std::ptrdiff_t>(consumed));
        return frames;
    }

    [[nodiscard]] bool peer_closed()const{return peer_closed_;}

private:
    std::vector<std::uint8_t>buffer_;
    bool peer_closed_{false};
};
void arm_timer_after(int timer_fd,SteadyClock::duration delay)
{
    auto remaining=std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    if(remaining<=std::chrono::nanoseconds::zero()){remaining=std::chrono::nanoseconds(1);}

    const auto seconds=std::chrono::duration_cast<std::chrono::seconds>(remaining);
    const auto nanoseconds=std::chrono::duration_cast<std::chrono::nanoseconds>(remaining-seconds);
    itimerspec specification{};
    specification.it_value.tv_sec=static_cast<time_t>(seconds.count());
    specification.it_value.tv_nsec=static_cast<long>(nanoseconds.count());
    if(::timerfd_settime(timer_fd,0,&specification,nullptr)!=0)
    {
        system_error("timerfd_settime");
    }
}

void arm_drain_timer(int timer_fd,SteadyClock::time_point hard_deadline)
{
    const auto now=SteadyClock::now();
    const auto hard_remaining=hard_deadline-now;
    const auto delay=std::min<SteadyClock::duration>(kDrainQuietPeriod,hard_remaining);
    arm_timer_after(timer_fd,delay);
}

bool consume_timer_expiration(int timer_fd)
{
    std::uint64_t expirations{};
    for(;;)
    {
        const auto count=::read(timer_fd,&expirations,sizeof(expirations));
        if(count==static_cast<ssize_t>(sizeof(expirations)))return true;
        if(count<0&&errno==EINTR)continue;
        if(count<0&&errno==EAGAIN)return false;
        if(count<0)
        {
            system_error("read timerfd");
        }
        throw std::runtime_error("short timerfd read");
    }
}

void disarm_timer(int timer_fd)
{
    itimerspec specification{};
    if(::timerfd_settime(timer_fd,0,&specification,nullptr)!=0)
    {
        system_error("timerfd_settime disarm");
    }
}

void add_epoll_interest(int epoll_fd,int fd)
{
    epoll_event event{};
    event.events=EPOLLIN;
    event.data.fd=fd;
    if(::epoll_ctl(epoll_fd,EPOLL_CTL_ADD,fd,&event)!=0)
    {
        system_error("epoll_ctl EPOLL_CTL_ADD");
    }
}

void usage(const char *program)
{
    std::cerr
        <<"Usage: "<<program
        <<" <multicast_ip> <udp_port> <output_directory>"
        <<" <proxy_ip> <control_port> <receiver_id> [interface_ip]\n"
        <<"Example: "<<program
        <<" 239.255.42.99 5000 received 127.0.0.1 6000 1 127.0.0.1\n";
}

struct ReceiverOptions
{
    std::string multicast_ip;
    int udp_port{};
    std::string output_directory;
    std::string proxy_ip;
    int control_port{};
    std::uint64_t receiver_id{};
    std::string interface_ip;
};

ReceiverOptions parse_options(int argc,char**argv)
{
    if(argc<7||argc>8)
    {
        usage(argv[0]);
        throw std::runtime_error("invalid receiver arguments");
    }

    ReceiverOptions options;
    options.multicast_ip=argv[1];
    options.udp_port=std::stoi(argv[2]);
    options.output_directory=argv[3];
    options.proxy_ip=argv[4];
    options.control_port=std::stoi(argv[5]);
    options.receiver_id=std::stoull(argv[6]);
    options.interface_ip=argc==8?argv[7]:"0.0.0.0";
    if(options.udp_port<1||options.udp_port>65535||
       options.control_port<1||options.control_port>65535||options.receiver_id==0)
    {
        throw std::runtime_error("invalid command-line argument");
    }
    return options;
}

FileDescriptor connect_control(const std::string &proxy_ip,int control_port)
{
    FileDescriptor control_fd(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0));
    if(control_fd.get()<0)
    {
        system_error("socket TCP control");
    }

    sockaddr_in proxy{};
    proxy.sin_family=AF_INET;
    proxy.sin_port=htons(static_cast<std::uint16_t>(control_port));
    if(::inet_pton(AF_INET,proxy_ip.c_str(),&proxy.sin_addr)!=1)
    {
        throw std::runtime_error("invalid proxy IPv4 address");
    }

    for(;;)
    {
        if(::connect(control_fd.get(),reinterpret_cast<sockaddr*>(&proxy),sizeof(proxy))==0)break;
        if(errno==EINTR)continue;
        system_error("connect proxy control");
    }

    return control_fd;
}

struct TransferState
{
    srcast::MetaPacket meta;
    std::string temporary_path;
    std::string output_path;
    std::string state_path;
    FileDescriptor output;
    std::vector<std::uint8_t>received;
    std::uint32_t received_count{0};
    std::uint64_t duplicate_count{0};
    std::uint64_t rejected_count{0};
    std::uint64_t intentional_drop_count{0};
    std::optional<std::uint32_t>active_section_id;
    std::optional<std::uint32_t>last_end_round;
    std::optional<std::uint32_t>last_reported_round;
    bool awaiting_complete_ack{false};
    std::unordered_set<std::uint32_t>intentionally_dropped_blocks;
};
//正式文件旁边放 .state，重启时和 .part 一起校验
std::string received_state_path(const std::string &output_path){return output_path+".state";}

//位图
std::string hex_bytes(const std::vector<std::uint8_t>&bytes)
{
    static constexpr char digits[]="0123456789abcdef";
    std::string result;
    result.reserve(bytes.size()*2U);
    for(const auto byte:bytes)
    {
        result.push_back(digits[(byte>>4U)&0x0fU]);
        result.push_back(digits[byte&0x0fU]);
    }
    return result;
}

std::optional<std::vector<std::uint8_t>>parse_hex_bytes(const std::string& hex)
{
    if(hex.size()%2U!=0U)return std::nullopt;

    auto value_of=[](char ch)->int
    {
        if(ch>='0'&&ch<='9')return ch-'0';
        if(ch>='a'&&ch<='f')return ch-'a'+10;
        if(ch>='A'&&ch<='F')return ch-'A'+10;
        return -1;
    };

    std::vector<std::uint8_t>bytes(hex.size()/2U);
    for(std::size_t index=0;index<bytes.size();index++)
    {
        const auto high=value_of(hex[index*2U]);
        const auto low=value_of(hex[index*2U+1U]);
        if(high<0||low<0)return std::nullopt;
        bytes[index]=static_cast<std::uint8_t>((high<<4U)|low);
    }
    return bytes;
}

//恢复时要同时检查 transfer_id、文件大小、摘要和位图计数。
bool load_receiver_state(TransferState &state,const srcast::MetaPacket &meta)
{
    if(!std::filesystem::exists(state.state_path)||!std::filesystem::exists(state.temporary_path))
    return false;

    std::error_code size_error;
    const auto temporary_size=std::filesystem::file_size(state.temporary_path,size_error);
    if(size_error||temporary_size!=meta.file_size)return false;

    std::ifstream input(state.state_path);
    if(!input)return false;

    std::string magic;
    int version=0;
    std::uint64_t transfer_id=0;
    std::uint64_t file_size=0;
    std::uint32_t total_blocks=0;
    std::uint32_t section_block_count=0;
    std::uint32_t received_count=0;
    std::string digest_hex;
    std::string received_hex;
    input>>magic>>version>>transfer_id>>file_size>>total_blocks>>section_block_count>>digest_hex
         >>received_count>>received_hex;
    auto received=parse_hex_bytes(received_hex);
    if(!input||!received||magic!="SRC_RECEIVER_STATE"||version!=2||transfer_id!=meta.common.transfer_id||
        file_size!=meta.file_size||total_blocks!=meta.total_blocks||
        section_block_count!=meta.section_block_count||digest_hex!=srcast::hex_digest(meta.sha256)||
        received->size()!=meta.total_blocks)return false;

    const auto counted=static_cast<std::uint32_t>(std::count(received->begin(),received->end(),1));
    if(counted!=received_count)return false;

    state.received=std::move(*received);
    state.received_count=received_count;
    return true;
}

//先写.tmp 再rename，避免state文件被写坏一半
void save_receiver_state(const TransferState &state)
{
    const auto temporary_state_path=state.state_path+".tmp";
    {
        std::ofstream output(temporary_state_path,std::ios::trunc);
        if(!output)
        {
            throw std::runtime_error("open receiver state failed: "+temporary_state_path);
        }
        output<<"SRC_RECEIVER_STATE 2\n"<<state.meta.common.transfer_id<<'\n'<<state.meta.file_size<<'\n'
        <<state.meta.total_blocks<<'\n'<<state.meta.section_block_count<<'\n'
        <<srcast::hex_digest(state.meta.sha256)<<'\n'<<state.received_count<<'\n'
        <<hex_bytes(state.received)<<'\n';
        output.flush();
        if(!output)
        {
            throw std::runtime_error("write receiver state failed: "+temporary_state_path);
        }
    }

    std::error_code error;
    std::filesystem::rename(temporary_state_path,state.state_path,error);
    if(error)
    {
        throw std::runtime_error("commit receiver state failed: "+error.message());
    }
}

//测试hook：只丢初始轮指定block，修复轮正常接
std::unordered_set<std::uint32_t>parse_initial_drop_blocks()
{
    std::unordered_set<std::uint32_t>blocks;
    const char *raw=std::getenv("SRCAST_DROP_INITIAL_BLOCKS");
    if(raw==nullptr||*raw=='\0')return blocks;

    const char*cursor=raw;
    while(*cursor!='\0')
    {
        char *end=nullptr;
        errno=0;
        const auto value=std::strtoul(cursor,&end,10);
        if(cursor==end||errno==ERANGE||value>std::numeric_limits<std::uint32_t>::max())
        {
            throw std::runtime_error("invalid SRCAST_DROP_INITIAL_BLOCKS value");
        }
        blocks.insert(static_cast<std::uint32_t>(value));
        if(*end=='\0')break;
        if(*end!=',')
        {
            throw std::runtime_error("SRCAST_DROP_INITIAL_BLOCKS must be a comma-separated list");
        }
        cursor=end+1;
        if(*cursor=='\0')
        {
            throw std::runtime_error("SRCAST_DROP_INITIAL_BLOCKS has a trailing comma");
        }
    }

    return blocks;
}

//初始化临时文件；如果 state 和 .part 匹配，就从已有位图继续。
bool initialize_transfer(TransferState &state,const srcast::MetaPacket &meta,
                         const std::string &output_directory)
{
    if(meta.block_size!=srcast::kPayloadSize)
    {
        std::cerr<<"reject META: unsupported block size\n";
        return false;
    }
    try
    {
        srcast::validate_section_block_count(meta.section_block_count);
    }catch(const std::exception&)
    {
        std::cerr<<"reject META: invalid section block count\n";
        return false;
    }

    const auto expected_blocks64=(meta.file_size+meta.block_size-1)/meta.block_size;
    if(expected_blocks64>std::numeric_limits<std::uint32_t>::max()||
       static_cast<std::uint32_t>(expected_blocks64)!=meta.total_blocks)
    {
        std::cerr<<"reject META: inconsistent file size and block count\n";
        return false;
    }

    state.meta=meta;
    const auto generated_name=std::string("transfer-")+std::to_string(meta.common.transfer_id)+".bin";
    state.output_path=(std::filesystem::path(output_directory)/generated_name).string();
    state.temporary_path=state.output_path+".part";
    state.state_path=received_state_path(state.output_path);
    state.received.assign(meta.total_blocks,0);
    state.received_count=0;
    state.duplicate_count=0;
    state.rejected_count=0;
    state.intentional_drop_count=0;
    state.active_section_id.reset();
    state.last_end_round.reset();
    state.last_reported_round.reset();
    state.awaiting_complete_ack=false;
    state.intentionally_dropped_blocks.clear();
    const bool loaded_state=load_receiver_state(state,meta);
    const int fd=::open(state.temporary_path.c_str(),O_CREAT|(loaded_state?0:O_TRUNC)|O_RDWR|O_CLOEXEC,
                        0644);
    if(fd<0)
    {
        system_error("open temporary output");
    }
    state.output.reset(fd);
    if(::ftruncate(state.output.get(),static_cast<off_t>(meta.file_size))!=0)
    {
        system_error("ftruncate");
    }

    std::cout<<"accepted transfer_id="<<meta.common.transfer_id<<'\n'
    <<"sections="<<srcast::section_count_for_blocks(meta.total_blocks,meta.section_block_count)<<'\n'
    <<"file_size="<<meta.file_size<<" bytes\n"<<"blocks="<<meta.total_blocks<<'\n'
    <<"recovered_blocks="<<state.received_count<<'\n'
    <<"expected_sha256="<<srcast::hex_digest(meta.sha256)<<'\n';
    return true;
}

//只统计当前section，避免每轮扫描文件
std::vector<std::uint8_t>make_missing_bitmap(const TransferState &state,std::uint32_t section_id,
                                             std::uint32_t &missing_count)
{
    const auto first_block=srcast::section_first_block(section_id,state.meta.section_block_count);
    const auto section_blocks=srcast::section_block_count(state.meta.total_blocks,section_id,
                                                          state.meta.section_block_count);
    std::vector<std::uint8_t>bitmap(srcast::bitmap_size_for_blocks(section_blocks),0);
    missing_count=0;
    for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
    {
        const auto block_id=first_block+local_block;
        if(state.received[block_id]==0)
        {
            srcast::bitmap_set(bitmap,local_block);
            missing_count++;
        }
    }
    return bitmap;
}

//最终提交检查文件SHA
bool finalize_transfer(TransferState &state,std::array<std::uint8_t,srcast::kSha256Size> &actual_digest)
{
    if(state.received_count!=state.meta.total_blocks)return false;
    if(::fsync(state.output.get())!=0)
    {
        system_error("fsync temporary output");
    }

    actual_digest=srcast::sha256_file(state.temporary_path);
    std::cout<<"actual_sha256="<<srcast::hex_digest(actual_digest)<<'\n';
    if(actual_digest!=state.meta.sha256)
    {
        std::cerr<<"final SHA-256 mismatch; temporary file retained at "<<state.temporary_path<<'\n';
        return false;
    }

    state.output.reset(-1);
    std::error_code error;
    std::filesystem::rename(state.temporary_path,state.output_path,error);
    if(error)
    {
        throw std::runtime_error("rename failed: "+error.message());
    }

    std::error_code remove_error;
    std::filesystem::remove(state.state_path,remove_error);
    std::cout<<"COMPLETED output="<<state.output_path<<" duplicates="<<state.duplicate_count
        <<" intentional_drops="<<state.intentional_drop_count<<" rejected="<<state.rejected_count<<'\n';
    return true;
}

//缺块，完成状态都从TCP控制回 proxy
void send_section_status(int control_fd,std::uint64_t receiver_id,const TransferState& state,
                        std::uint32_t section_id,std::uint32_t round_id,srcast::SectionStatusCode status,
                        std::uint32_t missing_count,std::vector<std::uint8_t>missing_bitmap)
{
    srcast::SectionStatusMessage message;
    message.receiver_id=receiver_id;
    message.transfer_id=state.meta.common.transfer_id;
    message.section_id=section_id;
    message.round_id=round_id;
    message.status=status;
    message.missing_count=missing_count;
    message.missing_bitmap=std::move(missing_bitmap);
    send_control_frame(control_fd,srcast::encode_section_status(message));
}

//DATA结束后等quiet period，内核处理UDP队列里的包
void finish_drain_and_report(int control_fd,std::uint64_t receiver_id,
                             TransferState &state,std::uint32_t round_id)
{
    if(!state.active_section_id)
    {
        throw std::runtime_error("no active section to report");
    }
    const auto section_id=*state.active_section_id;
    std::uint32_t missing_count{};
    auto missing_bitmap=make_missing_bitmap(state,section_id,missing_count);
    std::cout<<"DATA quiet period finished; round="<<round_id<<" section="<<section_id
             <<" missing_blocks="<<missing_count<<'\n';
    if(missing_count!=0)
    {
        send_section_status(control_fd,receiver_id,state,section_id,round_id,
                            srcast::SectionStatusCode::Missing,missing_count,std::move(missing_bitmap));
        state.last_reported_round=round_id;
        std::cout<<"missing bitmap sent; waiting for repair\n";
        return;
    }

    std::array<std::uint8_t,srcast::kSha256Size>actual_digest{};
    if(!finalize_transfer(state,actual_digest))
    {
        if(state.received_count!=state.meta.total_blocks)
        {
            send_section_status(control_fd,receiver_id,state,section_id,round_id,
                                srcast::SectionStatusCode::Complete,0,{});
            state.last_reported_round=round_id;
            std::cout<<"section complete; waiting for next section\n";
            return;
        }

        send_section_status(control_fd,receiver_id,state,section_id,round_id,
                            srcast::SectionStatusCode::Failed,0,{});
        state.last_reported_round=round_id;
        return;
    }

    send_section_status(control_fd,receiver_id,state,section_id,round_id,
                        srcast::SectionStatusCode::Complete,0,{});
    srcast::ReceiverCompleteMessage complete;
    complete.receiver_id=receiver_id;
    complete.transfer_id=state.meta.common.transfer_id;
    complete.file_size=state.meta.file_size;
    complete.sha256=actual_digest;
    send_control_frame(control_fd,srcast::encode_receiver_complete(complete));
    state.last_reported_round=round_id;
    state.awaiting_complete_ack=true;
    std::cout<<"completion confirmation sent; waiting for COMPLETE_ACK\n";
}

bool should_drop_initial_block_for_test(TransferState &state,
    const std::unordered_set<std::uint32_t>& configured_blocks,std::uint32_t block_id)
{
    if(configured_blocks.empty()||state.last_end_round)return false;
    if(configured_blocks.find(block_id)==configured_blocks.end())return false;
    const auto inserted=state.intentionally_dropped_blocks.insert(block_id);
    if(!inserted.second)return false;

    state.intentional_drop_count++;
    std::cout<<"test hook dropped initial DATA block_id="<<block_id<<'\n';
    return true;
}

//同一轮SECTION_END可能重复到，drain timer只保留当前轮
void begin_drain_for_round(TransferState &state,std::uint32_t section_id,std::uint32_t round_id,
    std::uint32_t total_blocks,int drain_timer_fd,bool &draining,std::uint32_t &draining_round,
    SteadyClock::time_point& hard_deadline)
{
    if(state.awaiting_complete_ack||total_blocks!=state.meta.total_blocks)
    {
        state.rejected_count++;
        return;
    }
    try
    {
        static_cast<void>(srcast::section_block_count(state.meta.total_blocks,section_id,
                          state.meta.section_block_count));
    }catch(const std::exception&)
    {
        state.rejected_count++;
        return;
    }

    if(!state.active_section_id||section_id!=*state.active_section_id)
    {
        if(state.active_section_id&&section_id<*state.active_section_id)return;
        state.active_section_id=section_id;
        state.last_end_round.reset();
        state.last_reported_round.reset();
    }
    if(state.last_reported_round&&round_id<=*state.last_reported_round)return;
    if(state.last_end_round&&round_id<*state.last_end_round)return;
    if(state.last_end_round&&round_id==*state.last_end_round&&draining)return;

    state.last_end_round=round_id;
    draining_round=round_id;
    draining=true;
    hard_deadline=SteadyClock::now()+kDrainHardTimeout;
    arm_drain_timer(drain_timer_fd,hard_deadline);
    std::cout<<"SECTION_END received section="<<section_id<<" round="<<round_id
             <<"; waiting for 200ms DATA quiet period"<<" (maximum 1s)\n";
}

//proxy的TCP控制消息都从这里进，UDP DATA 只负责文件块
void handle_control_message(const std::vector<std::uint8_t>&frame,int control_fd,
    std::uint64_t receiver_id,const std::string &output_directory,std::optional<TransferState>&transfer,
    int drain_timer_fd,bool &draining,std::uint32_t &draining_round,
    SteadyClock::time_point &hard_deadline,bool &stop_requested)
{
    const auto type=srcast::peek_control_type(frame);
    if(type==srcast::ControlType::SessionEnd)
    {
        srcast::decode_session_end(frame);
        if(transfer)
        std::cerr<<"SESSION_END received with an active transfer; preserving temporary state\n";
        stop_requested=true;
        return;
    }

    if(type==srcast::ControlType::FileMeta)
    {
        const auto message=srcast::decode_file_meta(frame);
//FILE_META只描述整文件，Section后面继续
        if(message.section_id!=srcast::kSingleSectionId)
        {
            throw std::runtime_error("unsupported FILE_META section_id");
        }

        if(message.block_size!=srcast::kPayloadSize)
        {
            throw std::runtime_error("unsupported FILE_META block size");
        }

//文件大小和block数对上
        const auto expected_blocks64=(message.file_size+message.block_size-1)/message.block_size;
        if(expected_blocks64>std::numeric_limits<std::uint32_t>::max()||
           static_cast<std::uint32_t>(expected_blocks64)!=message.total_blocks)
        {
            throw std::runtime_error("inconsistent FILE_META block count");
        }

//重复FILE_META直接回READY，避免重建 .part
        if(transfer)
        {
            if(transfer->meta.common.transfer_id==message.transfer_id)
            {
             send_control_frame(control_fd,srcast::encode_meta_ready({receiver_id,message.transfer_id}));
             return;
            }
            throw std::runtime_error("FILE_META received while another ""transfer is active");
        }

//内部复用MetaPacket，入口已经从UDP META换成TCP FILE_META
        srcast::MetaPacket meta;
        meta.common={srcast::PacketType::Meta,message.transfer_id};
        meta.file_size=message.file_size;
        meta.block_size=message.block_size;
        meta.total_blocks=message.total_blocks;
        meta.section_block_count=message.section_block_count;
        meta.sha256=message.sha256;
        const auto output_name=(std::filesystem::path(output_directory)/
                   ("transfer-"+std::to_string(meta.common.transfer_id)+".bin")).string();
        std::uint64_t existing_size=0;
        std::error_code existing_error;
        const auto temporary_name=output_name+".part";
        if(std::filesystem::exists(temporary_name,existing_error))
        {
            existing_size=std::filesystem::file_size(temporary_name,existing_error);
        }
        if(existing_error)
        {
            throw std::runtime_error("stat receiver temporary file failed: "+existing_error.message());
        }
        const auto additional=existing_size<meta.file_size?meta.file_size-existing_size:0;
        require_storage_limit(output_directory,additional,"SRCAST_OUTPUT_LIMIT_BYTES",
                             "receiver output directory");
        require_disk_space(output_directory,meta.file_size,"receiver output directory");
        transfer.emplace();
        if(!initialize_transfer(*transfer,meta,output_directory))
        {
            transfer.reset();
            throw std::runtime_error("failed to initialize FILE_META");
        }

//清上一文件可能留下的drain timer
        disarm_timer(drain_timer_fd);
        static_cast<void>(consume_timer_expiration(drain_timer_fd));
        draining=false;
        send_control_frame(control_fd,srcast::encode_meta_ready({receiver_id,message.transfer_id}));
        std::cout<<"FILE_META accepted;"<<"META_READY sent\n";
        return;
    }

    if(type==srcast::ControlType::SectionEnd)
    {
//先不上报，等UDP队列安静后算缺块
        const auto message=srcast::decode_section_end(frame);
        if(!transfer||transfer->meta.common.transfer_id!=message.transfer_id)return;
        begin_drain_for_round(*transfer,message.section_id,message.round_id,message.total_blocks,
                               drain_timer_fd,draining,draining_round,hard_deadline);
        return;
    }

    if(type==srcast::ControlType::RepairBegin)
    {
//修复轮开始提示，数据走UDP
        const auto message=srcast::decode_repair_begin(frame);
        if(!transfer||transfer->meta.common.transfer_id!=message.transfer_id||
            transfer->awaiting_complete_ack)return;
        try
        {
            static_cast<void>(srcast::section_block_count(transfer->meta.total_blocks,message.section_id,
                              transfer->meta.section_block_count));
        }catch(const std::exception&)
        {
            return;
        }
        std::cout<<"repair section "<<message.section_id<<" round "<<message.round_id<<" begins\n";
        return;
    }

    if(type==srcast::ControlType::BackfillBegin)
    {
//慢节点被隔离后，proxy会走TCP backfill
        const auto message=srcast::decode_backfill_begin(frame);
        if(!transfer||transfer->meta.common.transfer_id!=message.transfer_id||
            transfer->meta.total_blocks!=message.total_blocks||transfer->awaiting_complete_ack)return;
        std::cout<<"TCP backfill begins\n";
        return;
    }

    if(type==srcast::ControlType::BackfillData)
    {
//backfill和UDP DATA一样校验offset size crc
        const auto message=srcast::decode_backfill_data(frame);
        if(!transfer||transfer->meta.common.transfer_id!=message.transfer_id||
            transfer->awaiting_complete_ack||message.block_id>=transfer->meta.total_blocks)return;

       const auto expected_offset=static_cast<std::uint64_t>(message.block_id)*transfer->meta.block_size;
        const auto expected_size=static_cast<std::uint16_t>(std::min<std::uint64_t>(
                   transfer->meta.block_size,transfer->meta.file_size-expected_offset));
        if(message.offset!=expected_offset||message.payload_size!=expected_size||
           message.offset+message.payload_size>transfer->meta.file_size||srcast::crc32(
           message.payload.data(),message.payload.size())!=message.crc32)
        {
            transfer->rejected_count++;
            return;
        }

        if(transfer->received[message.block_id]==0)
        {
//写完数据再更新state，崩溃后重复收，不跳过
            write_all_at(transfer->output.get(),message.payload.data(),
                         message.payload.size(),message.offset);
            transfer->received[message.block_id]=1;
            transfer->received_count++;
            if(::fsync(transfer->output.get())!=0)
            {
                system_error("fsync TCP backfill block");
            }
            save_receiver_state(*transfer);
        }
        return;
    }

    if(type==srcast::ControlType::BackfillEnd)
    {
//TCP补齐后仍然文件SHA和原子rename
        const auto message=srcast::decode_backfill_end(frame);
        if(!transfer||transfer->meta.common.transfer_id!=message.transfer_id||
           transfer->meta.file_size!=message.file_size||transfer->meta.total_blocks!=message.total_blocks
           ||transfer->meta.sha256!=message.sha256||transfer->awaiting_complete_ack)return;

        std::array<std::uint8_t,srcast::kSha256Size>actual_digest{};
        if(!finalize_transfer(*transfer,actual_digest))
        {
            send_section_status(control_fd,receiver_id,*transfer,transfer->active_section_id.value_or(0),
                    transfer->last_reported_round.value_or(0),srcast::SectionStatusCode::Failed,0,{});
            transfer->last_reported_round=transfer->last_reported_round.value_or(0);
            return;
        }

        srcast::ReceiverCompleteMessage complete;
        complete.receiver_id=receiver_id;
        complete.transfer_id=transfer->meta.common.transfer_id;
        complete.file_size=transfer->meta.file_size;
        complete.sha256=actual_digest;
        send_control_frame(control_fd,srcast::encode_receiver_complete(complete));
        transfer->awaiting_complete_ack=true;
        std::cout<<"TCP backfill completed; waiting for COMPLETE_ACK\n";
        return;
    }

    if(type==srcast::ControlType::CompleteAck)
    {
//proxy ACK后，receiver才清理当前TransferState
        const auto message=srcast::decode_complete_ack(frame);
        if(!transfer||message.receiver_id!=receiver_id||
            message.transfer_id!=transfer->meta.common.transfer_id||!transfer->awaiting_complete_ack)
        return;

        const auto completed_transfer_id=message.transfer_id;
        disarm_timer(drain_timer_fd);
        static_cast<void>(consume_timer_expiration(drain_timer_fd));
        transfer.reset();
        draining=false;
       send_control_frame(control_fd,srcast::encode_receiver_ready({receiver_id,completed_transfer_id}));
        std::cout<<"COMPLETE_ACK received; RECEIVER_READY sent\n";
        return;
    }

    if(type==srcast::ControlType::TransferResult)
    {
//失败时保留 .part 和 .state
        const auto message=srcast::decode_transfer_result(frame);
        if(!transfer)
        {
            if(message.result==srcast::TransferResultCode::Failed)
            {
                send_control_frame(control_fd,srcast::encode_receiver_ready
                                 ({receiver_id,message.transfer_id}));
            }
            return;
        }
        if(message.transfer_id!=transfer->meta.common.transfer_id)return;

        if(message.result==srcast::TransferResultCode::Failed)
        {
            std::cout<<"transfer failed after maximum repair rounds;"
                     <<"temporary file retained at "<<transfer->temporary_path<<'\n';
            const auto failed_transfer_id=message.transfer_id;
            disarm_timer(drain_timer_fd);
            static_cast<void>(consume_timer_expiration(drain_timer_fd));
            transfer.reset();
            draining=false;
            send_control_frame(control_fd,srcast::encode_receiver_ready
                             ({receiver_id,failed_transfer_id}));
            std::cout<<"RECEIVER_READY sent for next transfer\n";
        }
        return;
    }

    std::cerr<<"ignore unexpected proxy control message\n";
}
}

int main(int argc,char**argv) try
{
    const auto options=parse_options(argc,argv);
    const auto &multicast_ip=options.multicast_ip;
    const auto udp_port=options.udp_port;
    const auto &output_directory=options.output_directory;
    const auto &proxy_ip=options.proxy_ip;
    const auto control_port=options.control_port;
    const auto receiver_id=options.receiver_id;
    const auto &interface_ip=options.interface_ip;

    std::error_code directory_error;
    std::filesystem::create_directories(output_directory,directory_error);
    if(directory_error)
    {
        throw std::runtime_error("create output directory failed: "+directory_error.message());
    }
    if(!std::filesystem::is_directory(output_directory))
    {
        throw std::runtime_error("output path is not a directory");
    }

    in_addr group_address{};
    in_addr interface_address{};
    if(::inet_pton(AF_INET,multicast_ip.c_str(),&group_address)!=1||
       !IN_MULTICAST(ntohl(group_address.s_addr)))
    {
        throw std::runtime_error("group must be an IPv4 multicast address");
    }
    if(::inet_pton(AF_INET,interface_ip.c_str(),&interface_address)!=1)
    {
        throw std::runtime_error("invalid interface IPv4 address");
    }

    FileDescriptor udp_fd(::socket(AF_INET,SOCK_DGRAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0));
    if(udp_fd.get()<0)
    {
        system_error("socket UDP");
    }

    const int reuse=1;
    if(::setsockopt(udp_fd.get(),SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse))!=0)
    {
        system_error("setsockopt UDP SO_REUSEADDR");
    }

    int receive_buffer=4*1024*1024;
    static_cast<void>(::setsockopt(udp_fd.get(),SOL_SOCKET,SO_RCVBUF,
                      &receive_buffer,sizeof(receive_buffer)));
    sockaddr_in local{};
    local.sin_family=AF_INET;
    local.sin_port=htons(static_cast<std::uint16_t>(udp_port));
    local.sin_addr.s_addr=htonl(INADDR_ANY);
    if(::bind(udp_fd.get(),reinterpret_cast<sockaddr*>(&local),sizeof(local))!=0)
    {
        system_error("bind UDP");
    }

    ip_mreq membership{};
    membership.imr_multiaddr=group_address;
    membership.imr_interface=interface_address;
    if(::setsockopt(udp_fd.get(),IPPROTO_IP,IP_ADD_MEMBERSHIP,&membership,sizeof(membership))!=0)
    {
        system_error("setsockopt IP_ADD_MEMBERSHIP");
    }

    auto control_fd=connect_control(proxy_ip,control_port);
    send_control_frame(control_fd.get(),srcast::encode_register
                     ({receiver_id,static_cast<std::uint16_t>(udp_port)}));
    std::cout<<"receiver_id="<<receiver_id<<" listening on "<<multicast_ip<<':'<<udp_port
             <<" via interface "<<interface_ip<<" control="<<proxy_ip<<':'<<control_port<<'\n';
    FileDescriptor drain_timer_fd(::timerfd_create(CLOCK_MONOTONIC,TFD_CLOEXEC|TFD_NONBLOCK));
    if(drain_timer_fd.get()<0)
    {
        system_error("timerfd_create");
    }

    FileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
    if(epoll_fd.get()<0)
    {
        system_error("epoll_create1");
    }

    add_epoll_interest(epoll_fd.get(),udp_fd.get());
    add_epoll_interest(epoll_fd.get(),drain_timer_fd.get());
    add_epoll_interest(epoll_fd.get(),control_fd.get());
    std::optional<TransferState>transfer;
    std::array<std::uint8_t,srcast::kMaxPacketSize>packet_buffer{};
    ControlStreamReader control_reader;
    const auto initial_drop_blocks=parse_initial_drop_blocks();
    if(!initial_drop_blocks.empty())
    {
        std::cout<<"test hook enabled: SRCAST_DROP_INITIAL_BLOCKS";
        for(const auto block_id:initial_drop_blocks)std::cout<<' '<<block_id;
        std::cout<<'\n';
    }

    bool draining=false;
    bool stop_requested=false;
    std::uint32_t draining_round=0;
    SteadyClock::time_point hard_deadline{};
    std::vector<epoll_event>ready_events(3);
    while(!stop_requested)
    {
        const int event_count=::epoll_wait(epoll_fd.get(),ready_events.data(),
                                           static_cast<int>(ready_events.size()),-1);
        if(event_count<0)
        {
            if(errno==EINTR)continue;
            system_error("epoll_wait");
        }

        bool udp_ready=false;
        bool timer_ready=false;
        bool control_ready=false;
        for(int index=0;index<event_count;index++)
        {
            const auto& event=ready_events[static_cast<std::size_t>(index)];
            const int ready_fd=event.data.fd;
            if((event.events&EPOLLERR)!=0U)
            {
                throw std::runtime_error("epoll descriptor error");
            }
            if((event.events&EPOLLIN)==0U)
            {
                if((event.events&EPOLLHUP)!=0U)
                {
                    throw std::runtime_error("epoll descriptor hangup");
                }
                continue;
            }

            if(ready_fd==udp_fd.get())udp_ready=true;
            else if(ready_fd==drain_timer_fd.get())timer_ready=true;
            else if(ready_fd==control_fd.get())control_ready=true;
            else
            {
                throw std::runtime_error("unknown descriptor returned by epoll");
            }
        }

        if(udp_ready)
        {
            std::size_t processed=0;
            const auto batch_start=SteadyClock::now();
//一次 epoll 唤醒只处理一批，UDP不能抢控制面时间
            while(processed<kMaxUdpPacketsPerBatch&&SteadyClock::now()-batch_start<kUdpBatchTimeBudget)
            {
                const auto packet_size=::recvfrom(udp_fd.get(),packet_buffer.data(),packet_buffer.size(),
                                                  0,nullptr,nullptr);
                if(packet_size<0)
                {
                    if(errno==EINTR)continue;
                    if(errno==EAGAIN||errno==EWOULDBLOCK)break;
                    system_error("recvfrom");
                }
                if(packet_size==0)continue;

                processed++;
                try
                {
                srcast::PacketReader preview(packet_buffer.data(),static_cast<std::size_t>(packet_size));
                    const auto common=srcast::read_common(preview);
                    if(common.type==srcast::PacketType::Meta)continue;//元数据TCP，UDP META 忽略
                    if(!transfer||common.transfer_id!=transfer->meta.common.transfer_id||
                        transfer->awaiting_complete_ack)continue;
                    if(common.type==srcast::PacketType::Data)
                    {
                        const auto data=srcast::decode_data(packet_buffer.data(),
                                                            static_cast<std::size_t>(packet_size));
                        auto& state=*transfer;
                        if(data.block_id>=state.meta.total_blocks||data.section_id!=
                           srcast::section_id_for_block(data.block_id,state.meta.section_block_count))
                        {
                            state.rejected_count++;
                            continue;
                        }

                        const auto expected_offset=static_cast<std::uint64_t>
                                                  (data.block_id)*state.meta.block_size;
                        const auto expected_size=static_cast<std::uint16_t>(std::min<std::uint64_t>(
                                    state.meta.block_size,state.meta.file_size-expected_offset));
                        if(data.offset!=expected_offset||data.payload_size!=expected_size||
                           data.offset+data.payload_size>state.meta.file_size)
                        {
                            state.rejected_count++;
                            continue;
                        }
                        if(srcast::crc32(data.payload,data.payload_size)!=data.crc32)
                        {
                            state.rejected_count++;
                            continue;
                        }
                        if(should_drop_initial_block_for_test(state,initial_drop_blocks,data.block_id))
                        continue;
                        if(state.received[data.block_id]!=0)
                        {
                            state.duplicate_count++;
                            continue;
                        }

//数据先落盘，fsync后再改位图和state
                        write_all_at(state.output.get(),data.payload,data.payload_size,data.offset);
                        state.received[data.block_id]=1;
                        state.received_count++;
                        if(::fsync(state.output.get())!=0)
                        {
                            system_error("fsync received block");
                        }
                        save_receiver_state(state);
                        if(draining)arm_drain_timer(drain_timer_fd.get(),hard_deadline);
                        if(state.received_count%1000U==0U||state.received_count==state.meta.total_blocks)
                        {
                            std::cout<<"received "<<state.received_count<<'/'<<state.meta.total_blocks
                            <<" blocks\n";
                        }
                        continue;
                    }

                    if(common.type==srcast::PacketType::End)
                    {
//UDP END是冗余提醒，真正上报是TCP SECTION_END
                        const auto end=srcast::decode_end(packet_buffer.data(),
                                                          static_cast<std::size_t>(packet_size));
                        begin_drain_for_round(*transfer,end.section_id,end.round_id,end.total_blocks,
                                             drain_timer_fd.get(),draining,draining_round,hard_deadline);
                        continue;
                    }
                }catch(const std::exception& error)
                {
                    if(transfer)transfer->rejected_count++;
                    std::cerr<<"drop malformed packet: "<<error.what()<<'\n';
                }
            }
        }

        if(timer_ready)
        {
            if(consume_timer_expiration(drain_timer_fd.get())&&draining&&transfer)
            {
                draining=false;
//quiet period到了，当前section round可以报状态
                finish_drain_and_report(control_fd.get(),receiver_id,*transfer,
                draining_round);
            }
        }

        if(control_ready)
        {
            const auto frames=control_reader.read_frames(control_fd.get());
            for(const auto& frame:frames)
            {
                handle_control_message(frame,control_fd.get(),receiver_id,output_directory,transfer,
                             drain_timer_fd.get(),draining,draining_round,hard_deadline,stop_requested);
            }
            if(control_reader.peer_closed()&&!stop_requested)
            {
                throw std::runtime_error("proxy control connection closed");
            }
        }
    }

    std::cout<<"proxy session ended cleanly\n";
    return 0;
}catch(const std::exception&error)
{
    std::cerr<<"receiver error: "<<error.what()<<'\n';
    return 1;
}
