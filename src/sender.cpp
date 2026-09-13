#include"checksum.hpp"
#include"control_io.hpp"
#include"file_descriptor.hpp"
#include"file_io.hpp"
#include"protocol.hpp"
#include"storage_utils.hpp"
#include"thread_pool.hpp"
#include"transfer_options.hpp"

#include<arpa/inet.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<poll.h>
#include<sys/epoll.h>
#include<sys/socket.h>
#include<sys/stat.h>
#include<sys/statvfs.h>
#include<unistd.h>
#include<algorithm>
#include<array>
#include<cerrno>
#include<chrono>
#include<cstdlib>
#include<cstdint>
#include<cstring>
#include<filesystem>
#include<fstream>
#include<iostream>
#include<limits>
#include<mutex>
#include<optional>
#include<random>
#include<sstream>
#include<stdexcept>
#include<string>
#include<thread>
#include<unordered_set>
#include<utility>
#include<vector>

namespace
{
using SteadyClock=std::chrono::steady_clock;
constexpr auto kReportTimeout=std::chrono::seconds(30);//等待接收端上报状态的最长时间
constexpr auto kMetaReadyTimeout=std::chrono::seconds(30);//等待接收端确认元数据的最长时间
constexpr auto kReadyTimeout=std::chrono::seconds(10);//等待接收端进入就绪状态的最长时间
constexpr auto kBackfillTimeout=std::chrono::seconds(30);//等待TCP补传结果的最长时间
constexpr std::uint32_t kSectionId=srcast::kSingleSectionId;//未分段流程使用的Section编号
constexpr std::size_t kMulticastRepairThreshold=2;//至少两个目标缺块时使用组播修复
constexpr std::size_t kBackfillThreadCount=4;//TCP补传线程池的最大线程数
constexpr int kMaxTransferRetries=2;//最终SHA失败后的整文件重试次数
std::mutex output_mutex;
int receiver_listener_fd=-1;

enum class DeliveryResult
{
    Success,
    Sha256Mismatch,
    Failed,
};
std::uint32_t backfill_rounds(int max_repair_rounds)
{
    const char* raw=std::getenv("SRCAST_BACKFILL_ROUNDS");
    if(raw==nullptr||*raw=='\0')return static_cast<std::uint32_t>(max_repair_rounds);

    char *end=nullptr;
    errno=0;
    const auto value=std::strtoul(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE||value>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error("invalid SRCAST_BACKFILL_ROUNDS value");
    }
    return static_cast<std::uint32_t>(value);
}

//测试 1 3 5 block
std::unordered_set<std::uint32_t>parse_block_set_env(const char *name)
{
    std::unordered_set<std::uint32_t>blocks;
    const char *raw=std::getenv(name);
    if(raw==nullptr||*raw=='\0')return blocks;

    const char *cursor=raw;
    while(*cursor!='\0')
    {
        char*end=nullptr;
        errno=0;
        const auto value=std::strtoul(cursor,&end,10);
        if(cursor==end||errno==ERANGE||value>std::numeric_limits<std::uint32_t>::max())
        {
            throw std::runtime_error(std::string("invalid ")+name+" value");
        }
        blocks.insert(static_cast<std::uint32_t>(value));
        if(*end=='\0')break;
        if(*end!=',')
        {
            throw std::runtime_error(std::string(name)+" must be a comma-separated list");
        }
        cursor=end+1;
        if(*cursor=='\0')
        {
            throw std::runtime_error(std::string(name)+" has a trailing comma");
        }
    }
    return blocks;
}

//环境变量非空且不是 0，认为打开
bool env_flag_enabled(const char*name)
{
    const char *raw=std::getenv(name);
    return raw!=nullptr&&*raw!='\0'&&std::string(raw)!="0";
}

std::uint64_t create_transfer_id()
{
    std::random_device device;
    std::mt19937_64 engine(device());
    return engine();
}

std::uint64_t create_transfer_id(const std::unordered_set<std::uint64_t>&used_transfer_ids)
{
    std::uint64_t transfer_id{};
    do
    {
        transfer_id=create_transfer_id();
    }while(used_transfer_ids.find(transfer_id)!=used_transfer_ids.end());
    return transfer_id;
}

void send_udp_packet(int socket_fd,const sockaddr_in &destination,const std::vector<std::uint8_t>&packet)
{
    const auto sent=::sendto(socket_fd,packet.data(),packet.size(),0,
                             reinterpret_cast<const sockaddr*>(&destination),sizeof(destination));
    if(sent<0)system_error("sendto");
    if(static_cast<std::size_t>(sent)!=packet.size())
    {
        throw std::runtime_error("sendto sent a partial datagram");
    }
}

struct CachedCentralFile
{
    std::string path;
    std::uint64_t transfer_id{};
    std::uint64_t file_size{};
    std::uint32_t total_blocks{};
    std::array<std::uint8_t,srcast::kSha256Size>sha256{};
};
//proxy的checkpoint跟最终缓存文件放一起，重启扫描
std::string central_checkpoint_path(const std::string &final_path){return final_path+".state";}

//只相信已经提交的section .part写半不算
std::uint32_t load_central_checkpoint(const std::string &checkpoint_path,
    const std::string &temporary_path,const srcast::CentralFileMetaMessage &meta)
{
    if(!std::filesystem::exists(checkpoint_path)||!std::filesystem::exists(temporary_path))return 0;

    std::error_code size_error;
    const auto temporary_size=std::filesystem::file_size(temporary_path,size_error);
    if(size_error||temporary_size!=meta.file_size)return 0;

    std::ifstream input(checkpoint_path);
    if(!input)return 0;

    std::string magic;
    int version=0;
    std::uint64_t transfer_id=0;
    std::uint64_t file_size=0;
    std::uint32_t total_blocks=0;
    std::uint32_t section_block_count=0;
    std::uint32_t next_section_id=0;
    std::string digest_hex;
    input>>magic>>version>>transfer_id>>file_size >>total_blocks
         >>section_block_count>>digest_hex>>next_section_id;
    const auto section_count=srcast::section_count_for_blocks(meta.total_blocks,meta.section_block_count);
    if(!input||magic!="SRC_PROXY_CHECKPOINT" ||version!=2||transfer_id!=meta.transfer_id||
        file_size!=meta.file_size||total_blocks!=meta.total_blocks||
        section_block_count!=meta.section_block_count||digest_hex!=srcast::hex_digest(meta.sha256)||
        next_section_id>=section_count)return 0;
    return next_section_id;
}

//checkpoint先写tmp再rename，proxy被kill不丢已提交section
void save_central_checkpoint(const std::string &checkpoint_path,const srcast::CentralFileMetaMessage &meta,
     std::uint32_t next_section_id)
{
    const auto temporary_checkpoint=checkpoint_path+".tmp";
    {
        std::ofstream output(temporary_checkpoint,std::ios::trunc);
        if(!output)
        {
            throw std::runtime_error("open central checkpoint failed: "+temporary_checkpoint);
        }
        output<<"SRC_PROXY_CHECKPOINT 2\n"<<meta.transfer_id<<'\n'<<meta.file_size<<'\n'
        <<meta.total_blocks<<'\n'<<meta.section_block_count<<'\n'<<srcast::hex_digest(meta.sha256)<<'\n'
        <<next_section_id<<'\n';
        output.flush();
        if(!output)
        {
            throw std::runtime_error("write central checkpoint failed: "+temporary_checkpoint);
        }
    }

    std::error_code error;
    std::filesystem::rename(temporary_checkpoint,checkpoint_path,error);
    if(error)
    {
        throw std::runtime_error("commit central checkpoint failed: "+error.message());
    }
}

//一tcp控制连接对应一个receiver
struct ReceiverConnection
{
    std::string receiver_endpoint;
    FileDescriptor control_fd;
    sockaddr_in udp_destination{};
    bool meta_ready{false};
    bool status_received{false};
    bool complete_received{false};
    bool completed{false};
    bool ready_for_next_transfer{false};
    bool isolated{false};
    bool abandoned{false};
    srcast::ReceiverState recovery_state{srcast::ReceiverState::Idle};
    std::uint64_t recovery_transfer_id{};
    std::vector<std::uint32_t>backfill_blocks;
    std::optional<srcast::SectionStatusMessage>status;
    std::optional<srcast::FileMetaMessage>active_meta;
};

std::string ipv4_address_string(const in_addr &address)
{
    char buffer[INET_ADDRSTRLEN]{};
    if(::inet_ntop(AF_INET,&address,buffer,sizeof(buffer))==nullptr)return "?";
    return buffer;
}

std::string receiver_endpoint_key(const sockaddr_in &peer,std::uint16_t udp_port)
{
    return ipv4_address_string(peer.sin_addr)+":"+std::to_string(udp_port);
}

void reset_transfer_state(std::vector<ReceiverConnection>&receivers)
{
    for(auto &receiver:receivers)
    {
        receiver.meta_ready=false;
        receiver.status_received=false;
        receiver.complete_received=false;
        receiver.completed=false;
        receiver.ready_for_next_transfer=false;
        receiver.isolated=false;
        receiver.abandoned=false;
        receiver.backfill_blocks.clear();
        receiver.status.reset();
    }
}

bool receiver_reported_sha_mismatch(const std::vector<ReceiverConnection>&receivers)
{
    return std::any_of(receivers.begin(),receivers.end(),[](const ReceiverConnection& receiver)
    {
        return receiver.status&&receiver.status->status==srcast::SectionStatusCode::Failed;
    });
}

bool send_receiver_frame(ReceiverConnection&receiver,const std::vector<std::uint8_t>&frame,
                         const std::string&operation)
{
    if(receiver.control_fd.get()<0)return false;
    try
    {
        send_control_frame(receiver.control_fd.get(),frame);
        return true;
    }catch(const std::exception&error)
    {
        std::cerr<<"receiver control connection failed during "<<operation
                 <<" receiver="<<receiver.receiver_endpoint<<": "<<error.what()<<'\n';
        receiver.control_fd.reset(-1);
        return false;
    }
}

std::optional<std::vector<std::uint8_t>>receive_receiver_frame(ReceiverConnection&receiver,
                                                               const std::string&operation)
{
    if(receiver.control_fd.get()<0)return std::nullopt;
    try
    {
        return receive_control_frame(receiver.control_fd.get());
    }catch(const std::exception&error)
    {
        std::cerr<<"receiver control connection failed during "<<operation
                 <<" receiver="<<receiver.receiver_endpoint<<": "<<error.what()<<'\n';
        receiver.control_fd.reset(-1);
        return std::nullopt;
    }
}

void abandon_incomplete_receivers(std::vector<ReceiverConnection>&receivers)
{
    for(auto &receiver:receivers)
    {
        if(receiver.completed)continue;
        std::cerr<<"receiver transfer failed; abandoning receiver="<<receiver.receiver_endpoint<<'\n';
        receiver.abandoned=true;
        receiver.control_fd.reset(-1);
    }
}

void usage(const char *program)
{
    std::cerr
        <<"Usage: "<<program
        <<" <multicast_ip> <udp_port> <interface_ip> <control_port>"
        <<" <receiver_count> <pace_us> <gap_ms> <max_repair_rounds>"
        <<" <file1> [file2 ...]\n"
        <<"   or: "<<program
        <<" <multicast_ip> <udp_port> <interface_ip> <control_port>"
        <<" <receiver_count> <pace_us> <gap_ms> <max_repair_rounds>"
        <<" --central-listen <central_port> <cache_dir>\n"
        <<"Example: "<<program
        <<" 239.255.42.99 5000 127.0.0.1 6000 3 200 1500 3"
        <<" input-a.bin input-b.bin\n";
}

struct SenderOptions
{
    std::string multicast_ip;
    int udp_port{};
    std::string interface_ip;
    int control_port{};
    int receiver_count{};
    int pace_us{};
    int gap_ms{};
    int max_repair_rounds{};
    std::vector<std::string>files;
    std::uint32_t section_block_limit{};
    bool central_mode{};
    int central_port{};
    std::string cache_directory;
};

SenderOptions parse_options(int argc,char**argv)
{
    if(argc<10)
    {
        usage(argv[0]);
        throw std::runtime_error("not enough command-line arguments");
    }

    SenderOptions options;
    options.multicast_ip=argv[1];
    options.udp_port=std::stoi(argv[2]);
    options.interface_ip=argv[3];
    options.control_port=std::stoi(argv[4]);
    options.receiver_count=std::stoi(argv[5]);
    options.pace_us=std::stoi(argv[6]);
    options.gap_ms=std::stoi(argv[7]);
    options.max_repair_rounds=std::stoi(argv[8]);
    options.central_mode=argc==12&&std::string(argv[9])=="--central-listen";
    options.central_port=options.central_mode?std::stoi(argv[10]):0;
    options.cache_directory=options.central_mode?argv[11]:"";
    if(!options.central_mode&&std::string(argv[9])=="--central-listen")
    {
        usage(argv[0]);
        throw std::runtime_error("invalid central-listen arguments");
    }
    for(int index=9;index<argc;index++)options.files.emplace_back(argv[index]);
    if(options.udp_port<1||options.udp_port>65535||options.control_port<1||options.control_port>65535||
       options.receiver_count<1||options.pace_us<0||options.gap_ms<0||options.max_repair_rounds<0||
      (options.central_mode&&(options.central_port<1||options.central_port>65535)))
    {
        throw std::runtime_error("invalid command-line argument");
    }
    options.section_block_limit=section_blocks_from_env();
    return options;
}

//创建tcp控制面监听socket
FileDescriptor create_control_listener(int control_port)
{
    FileDescriptor listener(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0));
    if(listener.get()<0)
    {
        system_error("socket TCP listener");
    }

    const int reuse=1;
    if(::setsockopt(listener.get(),SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse))!=0)
    {
        system_error("setsockopt TCP SO_REUSEADDR");
    }

    sockaddr_in local{};
    local.sin_family=AF_INET;
    local.sin_port=htons(static_cast<std::uint16_t>(control_port));
    local.sin_addr.s_addr=htonl(INADDR_ANY);
    if(::bind(listener.get(),reinterpret_cast<sockaddr*>(&local),sizeof(local))!=0)
    {
        system_error("bind TCP listener");
    }
    if(::listen(listener.get(),64)!=0)
    {
        system_error("listen TCP listener");
    }
    return listener;
}

void set_receive_timeout(int fd,std::chrono::seconds timeout)
{
    timeval value{};
    value.tv_sec=timeout.count();
    value.tv_usec=0;
    if(::setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&value,sizeof(value))!=0)
    {
        system_error("setsockopt SO_RCVTIMEO");
    }
}

struct AcceptedReceiver
{
    srcast::RegisterMessage registration;
    FileDescriptor control_fd;
    sockaddr_in peer{};
    std::string receiver_endpoint;
};

AcceptedReceiver accept_receiver_registration(int listener_fd)
{
    sockaddr_in peer{};
    socklen_t peer_size=sizeof(peer);
    int accepted=-1;
    for(;;)
    {
        accepted=::accept4(listener_fd,reinterpret_cast<sockaddr*>(&peer),&peer_size,SOCK_CLOEXEC);
        if(accepted>=0)break;
        if(errno==EINTR)continue;
        system_error("accept receiver");
    }

    FileDescriptor control_fd(accepted);
    //连接后等2s，接收端必须发信息
    set_receive_timeout(control_fd.get(),std::chrono::seconds(2));
    const auto registration=srcast::decode_register(receive_control_frame(control_fd.get()));
    //tcp恢复阻塞
    set_receive_timeout(control_fd.get(),std::chrono::seconds(0));
    if(registration.udp_port==0)
    {
        throw std::runtime_error("receiver UDP port must not be zero");
    }else if(registration.state==srcast::ReceiverState::Idle&&registration.current_transfer_id!=0)
    {
        throw std::runtime_error("IDLE receiver has a current transfer");
    }else if(registration.state!=srcast::ReceiverState::Idle&&registration.current_transfer_id==0)
    {
        throw std::runtime_error("active receiver has no current transfer");
    }
    return {registration,std::move(control_fd),peer,receiver_endpoint_key(peer,registration.udp_port)};
}

void install_receiver_connection(ReceiverConnection &receiver,AcceptedReceiver accepted,bool reconnect)
{
    const bool was_completed=receiver.completed;
    const bool was_abandoned=receiver.abandoned;
    receiver.control_fd=std::move(accepted.control_fd);//新连接接管前关闭旧fd
    receiver.receiver_endpoint=accepted.receiver_endpoint;
    receiver.udp_destination=accepted.peer;
    receiver.udp_destination.sin_port=htons(accepted.registration.udp_port);
    receiver.recovery_state=accepted.registration.state;
    receiver.recovery_transfer_id=accepted.registration.current_transfer_id;

    std::cout<<(reconnect?"reconnected":"registered")<<" receiver="
             <<receiver.receiver_endpoint<<" state=";
    if(receiver.recovery_state==srcast::ReceiverState::Idle)std::cout<<"IDLE";
    else if(receiver.recovery_state==srcast::ReceiverState::Receiving)std::cout<<"RECEIVING";
    else std::cout<<"COMPLETED";
    std::cout<<'\n'<<std::flush;

    if(!receiver.active_meta)return;
    const auto &meta=*receiver.active_meta;
    if(receiver.recovery_state==srcast::ReceiverState::Completed&&
       receiver.recovery_transfer_id==meta.transfer_id)
    {
        receiver.abandoned=false;
        receiver.meta_ready=true;
        receiver.status_received=true;
        receiver.complete_received=true;
        receiver.completed=true;
        send_receiver_frame(receiver,srcast::encode_complete_ack
                           ({meta.transfer_id}),"recovered COMPLETE_ACK");
        std::cout<<"recovered completed receiver="<<receiver.receiver_endpoint<<'\n';
        return;
    }

    if(receiver.recovery_state==srcast::ReceiverState::Idle&&was_completed)
    {
        receiver.ready_for_next_transfer=true;
        return;
    }

    if(was_abandoned)
    {
        if(receiver.recovery_state==srcast::ReceiverState::Idle)
        receiver.ready_for_next_transfer=true;
        else send_receiver_frame(receiver,srcast::encode_transfer_result
                                ({meta.transfer_id,srcast::TransferResultCode::Failed}),
                                "abandoned transfer result");
        return;
    }

    if(receiver.recovery_state==srcast::ReceiverState::Completed)
    {
        send_receiver_frame(receiver,srcast::encode_complete_ack
                           ({receiver.recovery_transfer_id}),
                            "stale COMPLETE_ACK");
    }else if(receiver.recovery_state==srcast::ReceiverState::Receiving&&
             receiver.recovery_transfer_id!=meta.transfer_id)
    {
        send_receiver_frame(receiver,srcast::encode_transfer_result
                           ({receiver.recovery_transfer_id,srcast::TransferResultCode::Failed}),
                            "stale transfer result");
    }

    receiver.meta_ready=false;
    receiver.status_received=false;
    receiver.complete_received=false;
    receiver.completed=false;
    receiver.ready_for_next_transfer=false;
    receiver.isolated=false;
    receiver.status.reset();
    receiver.recovery_state=srcast::ReceiverState::Receiving;
    receiver.recovery_transfer_id=meta.transfer_id;
    send_receiver_frame(receiver,srcast::encode_file_meta(meta),"reconnected FILE_META");
}

//等齐固定接收端，坏注册丢，重复身份只替换旧连接
//注册2s，防止tcp连接占启动流程
std::vector<ReceiverConnection>accept_receivers(int listener_fd,std::size_t expected_receivers)
{
    std::vector<ReceiverConnection>receivers;
    receivers.reserve(expected_receivers);
    std::cout<<"waiting for "<<expected_receivers<<" receiver control connections\n"<<std::flush;
    while(receivers.size()<expected_receivers)
    {
        try
        {
            auto accepted=accept_receiver_registration(listener_fd);
            const auto found=std::find_if(receivers.begin(),receivers.end(),[&](const auto &receiver)
            {
                return receiver.receiver_endpoint==accepted.receiver_endpoint;
            });
            //出现相同身份可能是接收端断线重连
            if(found!=receivers.end())install_receiver_connection(*found,std::move(accepted),true);
            else
            {
                ReceiverConnection receiver;
                receiver.receiver_endpoint=accepted.receiver_endpoint;
                install_receiver_connection(receiver,std::move(accepted),false);
                receivers.push_back(std::move(receiver));
            }
        }catch(const std::exception &error)
        {
            std::cerr<<"rejected receiver registration: "<<error.what()<<'\n';
        }
    }
    return receivers;
}

void accept_receiver_reconnect(std::vector<ReceiverConnection>&receivers)
{
    try
    {
        auto accepted=accept_receiver_registration(receiver_listener_fd);
        const auto found=std::find_if(receivers.begin(),receivers.end(),[&](const auto&receiver)
        {
            return receiver.receiver_endpoint==accepted.receiver_endpoint;
        });
        if(found==receivers.end())
        {
            std::cerr<<"rejected receiver registration: receiver is not a fixed member\n";
            return;
        }
        install_receiver_connection(*found,std::move(accepted),true);
    }catch(const std::exception&error)
    {
        std::cerr<<"rejected receiver registration: "<<error.what()<<'\n';
    }
}

void accept_pending_receiver_reconnects(std::vector<ReceiverConnection>&receivers)
{
    if(receiver_listener_fd<0)return;
    for(;;)
    {
        pollfd event{};
        event.fd=receiver_listener_fd;
        event.events=POLLIN;
        const auto ready=::poll(&event,1,0);
        if(ready<0)
        {
            if(errno==EINTR)continue;
            system_error("poll receiver listener");
        }
        if(ready==0||(event.revents&POLLIN)==0)return;
        accept_receiver_reconnect(receivers);
    }
}

//缺块位图
bool validate_missing_bitmap(const srcast::SectionStatusMessage &status,std::uint32_t section_blocks)
{
    const auto expected_size=srcast::bitmap_size_for_blocks(section_blocks);
    if(status.status==srcast::SectionStatusCode::Missing)
    {
        if(status.missing_bitmap.size()!=expected_size||status.missing_count==0)return false;
        std::uint32_t counted=0;
        for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
        {
            if(srcast::bitmap_test(status.missing_bitmap,local_block))counted++;
        }

        if(section_blocks%8U!=0U&&!status.missing_bitmap.empty())
        {
            const auto used_bits=static_cast<unsigned int>(section_blocks%8U);
            const auto unused_mask=static_cast<std::uint8_t>(0xffU<<used_bits);
            if((status.missing_bitmap.back()&unused_mask)!=0U)return false;
        }
        return counted==status.missing_count;
    }

    return status.missing_count==0&&status.missing_bitmap.empty();
}

void reset_round_state(std::vector<ReceiverConnection>&receivers)
{
    for(auto& receiver:receivers)
    {
        receiver.status_received=false;
        receiver.complete_received=false;
        receiver.ready_for_next_transfer=false;
        receiver.status.reset();
    }
}

//最后一段等COMPLETE，中间段只需要缺块位图
bool round_responses_complete(const std::vector<ReceiverConnection>&receivers,bool final_section)
{
    for(const auto &receiver:receivers)
    {
        if(receiver.completed||receiver.isolated||receiver.abandoned)continue;
        if(!receiver.status_received)return false;
        if(final_section&&receiver.status&&receiver.status->status==srcast::SectionStatusCode::Complete
         &&!receiver.complete_received)return false;
    }
    return true;
}

//控制消息可能晚到，先按transfer/round过滤
//收到COMPLETE时才回COMPLETE_ACK，receiver可以安全退出当前文件
void handle_control_frame(ReceiverConnection &receiver,const std::vector<std::uint8_t>&frame,
                          std::uint64_t transfer_id,std::uint32_t section_id,std::uint32_t round_id,
                          std::uint64_t file_size,std::uint32_t section_blocks,
                          const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    const auto type=srcast::peek_control_type(frame);
    if(type==srcast::ControlType::SectionStatus)
    {
        const auto status=srcast::decode_section_status(frame);
        if(status.transfer_id!=transfer_id||status.section_id!=section_id||
           status.round_id!=round_id)
        {
            std::cerr<<"ignore stale or mismatched SECTION_STATUS from receiver="
                     <<receiver.receiver_endpoint<<'\n';
            return;
        }
        if(!validate_missing_bitmap(status,section_blocks))
        {
            std::cerr<<"reject invalid missing bitmap from receiver="<<receiver.receiver_endpoint<<'\n';
            return;
        }

        receiver.status=status;
        receiver.status_received=true;
        std::cout<<"section="<<section_id<<" round="<<round_id
                 <<" receiver="<<receiver.receiver_endpoint<<" status=";
        if(status.status==srcast::SectionStatusCode::Complete)std::cout<<"COMPLETE";
        else if(status.status==srcast::SectionStatusCode::Missing)
        std::cout<<"MISSING missing_blocks="<<status.missing_count;
        else std::cout<<"FAILED";
        std::cout<<'\n';
        return;
    }

    if(type==srcast::ControlType::ReceiverComplete)
    {
        const auto complete=srcast::decode_receiver_complete(frame);
        if(!receiver.status_received||!receiver.status||
            receiver.status->status!=srcast::SectionStatusCode::Complete||
            complete.transfer_id!=transfer_id||complete.file_size!=file_size||
            complete.sha256!=digest)
        {
            std::cerr<<"reject invalid RECEIVER_COMPLETE from receiver="<<receiver.receiver_endpoint<<'\n';
            return;
        }

        receiver.complete_received=true;
        receiver.completed=true;
        receiver.recovery_state=srcast::ReceiverState::Completed;
        receiver.recovery_transfer_id=transfer_id;
        send_receiver_frame(receiver,srcast::encode_complete_ack
                           ({transfer_id}),"COMPLETE_ACK");
        std::cout<<"completion acknowledged receiver="<<receiver.receiver_endpoint<<'\n';
        return;
    }

    std::cerr<<"ignore unexpected control message from receiver="<<receiver.receiver_endpoint<<'\n';
}

template<typename Predicate>//临时建epoll
std::vector<std::size_t>wait_for_receiver_events(std::vector<ReceiverConnection>&receivers,
     Predicate should_watch,int timeout_ms,const std::string &operation)
{
    FileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
    if(epoll_fd.get()<0)
    {
        system_error("epoll_create1 "+operation);
    }

    std::size_t watched_count=0;
    std::vector<int>watched_fds(receivers.size(),-1);
    for(std::size_t index=0;index<receivers.size();index++)
    {
        if(!should_watch(receivers[index])||receivers[index].control_fd.get()<0)continue;
        epoll_event event{};
        event.events=EPOLLIN|EPOLLRDHUP;
        event.data.u64=static_cast<std::uint64_t>(index);
        if(::epoll_ctl(epoll_fd.get(),EPOLL_CTL_ADD,receivers[index].control_fd.get(),&event)!=0)
        {
            system_error("epoll_ctl add "+operation);
        }
        watched_fds[index]=receivers[index].control_fd.get();
        watched_count++;
    }

    constexpr auto listener_index=std::numeric_limits<std::uint64_t>::max();
    if(receiver_listener_fd>=0)
    {
        epoll_event event{};
        event.events=EPOLLIN;
        event.data.u64=listener_index;
        if(::epoll_ctl(epoll_fd.get(),EPOLL_CTL_ADD,receiver_listener_fd,&event)!=0)
        {
            system_error("epoll_ctl add receiver listener "+operation);
        }
        watched_count++;
    }

    if(watched_count==0)return {};

    std::vector<epoll_event>events(watched_count);
    int ready=0;
    for(;;)
    {
        ready=::epoll_wait(epoll_fd.get(),events.data(),static_cast<int>(events.size()),timeout_ms);
        if(ready>=0)break;
        if(errno==EINTR)continue;
        system_error("epoll_wait "+operation);
    }

    std::vector<std::size_t>indexes;
    indexes.reserve(static_cast<std::size_t>(ready));
    bool listener_ready=false;
    for(int event_index=0;event_index<ready;event_index++)
    {
        const auto &event=events[static_cast<std::size_t>(event_index)];
        if(event.data.u64==listener_index)
        {
            listener_ready=(event.events&EPOLLIN)!=0U;
            continue;
        }
        const auto index=static_cast<std::size_t>(event.data.u64);
        if((event.events&EPOLLIN)!=0U)
        {
            indexes.push_back(index);
            continue;
        }
        if((event.events&(EPOLLERR|EPOLLHUP|EPOLLRDHUP))!=0U&&
           index<receivers.size()&&receivers[index].control_fd.get()==watched_fds[index])
        {
            std::cerr<<"receiver control disconnected during "<<operation
                     <<" receiver="<<receivers[index].receiver_endpoint<<'\n';
            receivers[index].control_fd.reset(-1);
        }
    }
    if(listener_ready)accept_receiver_reconnect(receivers);
    return indexes;
}

//使用epoll时等待所有接收端的本轮状态报告
//超时不是失败，后面慢接收端逻辑会处理
void collect_round_reports(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id,
     std::uint32_t section_id,std::uint32_t round_id,std::uint64_t file_size,std::uint32_t total_blocks,
     std::uint32_t section_block_limit,const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    reset_round_state(receivers);
    const auto deadline=SteadyClock::now()+kReportTimeout;
    const auto final_section=section_id+1U==srcast::section_count_for_blocks(total_blocks,
                                                                             section_block_limit);
    const auto section_blocks=srcast::section_block_count(total_blocks,section_id,section_block_limit);
    while(!round_responses_complete(receivers,final_section))
    {
        const auto now=SteadyClock::now();
        if(now>=deadline)break;

        const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(deadline-now);
        const auto timeout_ms=static_cast<int>(std::max<std::int64_t>(1,remaining.count()));
        const auto ready_indexes=wait_for_receiver_events(receivers,
            [](const ReceiverConnection &receiver)
            {return !receiver.completed&&!receiver.isolated&&!receiver.abandoned;},
            timeout_ms,"receiver reports");
        if(ready_indexes.empty())continue;

        for(const auto receiver_index:ready_indexes)
        {
            auto &receiver=receivers[receiver_index];
            const auto frame=receive_receiver_frame(receiver,"receiver reports");
            if(!frame)continue;
            handle_control_frame(receiver,*frame,transfer_id,section_id,
                                 round_id,file_size,section_blocks,digest);
        }
    }

    for(const auto &receiver:receivers)
    {
        if(!receiver.completed&&!receiver.isolated&&!receiver.abandoned&&!receiver.status_received)
        {
            std::cout<<"section="<<section_id<<" round="<<round_id
                     <<" receiver="<<receiver.receiver_endpoint<<" status=NO_REPORT\n";
        }
    }
}

//达到配置修复轮数后仍缺块，转入慢节点队列
void isolate_slow_receivers(std::vector<ReceiverConnection>&receivers,std::uint32_t total_blocks,
     std::uint32_t section_id,std::uint32_t round_id,std::uint32_t section_block_limit,
     std::uint32_t backfill_after_rounds)
{
    const auto first_block=srcast::section_first_block(section_id,section_block_limit);
    const auto section_blocks=srcast::section_block_count(total_blocks,section_id,section_block_limit);
    for(auto &receiver:receivers)
    {
        if(receiver.completed||receiver.isolated||receiver.abandoned||!receiver.status||
           receiver.status->status!=srcast::SectionStatusCode::Missing||
           round_id<backfill_after_rounds)continue;

        for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
        {
            if(srcast::bitmap_test(receiver.status->missing_bitmap,local_block))
            receiver.backfill_blocks.push_back(first_block+local_block);
        }

        std::sort(receiver.backfill_blocks.begin(),receiver.backfill_blocks.end());
        receiver.backfill_blocks.erase(std::unique(receiver.backfill_blocks.begin(),
                   receiver.backfill_blocks.end()),receiver.backfill_blocks.end());
        receiver.isolated=true;
        std::cout<<"receiver="<<receiver.receiver_endpoint<<
        " isolated for TCP backfill missing_blocks="<<receiver.status->missing_count
        <<" section="<<section_id<<" repair_rounds="<<round_id<<'\n';
    }
}

//block->receiver列表，修复时决定组播单播
std::vector<std::vector<std::size_t>>aggregate_missing_blocks(
    const std::vector<ReceiverConnection>&receivers,std::uint32_t total_blocks,std::uint32_t section_id,
    std::uint32_t section_block_limit)
{
    const auto section_blocks=srcast::section_block_count(total_blocks,section_id,section_block_limit);
    std::vector<std::vector<std::size_t>>missing_by_block(section_blocks);
    for(std::size_t receiver_index=0;receiver_index<receivers.size();receiver_index++)
    {
        const auto& receiver=receivers[receiver_index];
        if(receiver.completed||receiver.isolated||receiver.abandoned||!receiver.status||
           receiver.status->status!=srcast::SectionStatusCode::Missing)continue;

        for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
        {
            if(srcast::bitmap_test(receiver.status->missing_bitmap,local_block))
            missing_by_block[local_block].push_back(receiver_index);
        }
    }
    return missing_by_block;
}

//UDP END多发，真正的排上报靠TCP SECTION_END
//receiver收到SECTION_END等quiet period，再计算缺块
void send_end_round(int udp_fd,const sockaddr_in &multicast_destination,
     std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id,std::uint32_t section_id,
     std::uint32_t round_id,std::uint32_t total_blocks)
{
    const auto end_packet=srcast::encode_end(transfer_id,section_id,round_id,total_blocks);
    for(int repetition=0;repetition<10;repetition++)
    {
        send_udp_packet(udp_fd,multicast_destination,end_packet);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    const auto section_end=srcast::encode_section_end({transfer_id,section_id,round_id,total_blocks});
    for(auto &receiver:receivers)
    {
        if(!receiver.completed&&!receiver.isolated&&!receiver.abandoned)
        send_receiver_frame(receiver,section_end,"SECTION_END");
    }
}

//本地修复，多机同缺组播，单机缺块单播
//修复包从proxy缓存读，不回中心拿。
void send_repair_round(int udp_fd,const sockaddr_in &multicast_destination,
     std::vector<ReceiverConnection>&receivers,int input_fd,const std::string &file_path,
     std::uint64_t file_size,std::uint64_t transfer_id,std::uint32_t total_blocks,std::uint32_t section_id,
     std::uint32_t round_id,int pace_us,std::uint32_t section_block_limit)
{
    const auto repair_begin=srcast::encode_repair_begin({transfer_id,section_id,round_id});
    for(auto &receiver:receivers)
    {
        if(!receiver.completed&&!receiver.isolated&&!receiver.abandoned)
        send_receiver_frame(receiver,repair_begin,"REPAIR_BEGIN");
    }

    const auto missing_by_block=aggregate_missing_blocks(receivers,total_blocks,
                                                         section_id,section_block_limit);
    std::array<std::uint8_t,srcast::kPayloadSize>buffer{};
    std::uint64_t multicast_packets=0;
    std::uint64_t unicast_packets=0;
    const auto first_block=srcast::section_first_block(section_id,section_block_limit);
    const auto section_blocks=srcast::section_block_count(total_blocks,section_id,section_block_limit);
    for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
    {
        const auto &targets=missing_by_block[local_block];
        if(targets.empty())continue;
        const auto block_id=first_block+local_block;
        std::uint64_t offset{};
        std::uint16_t payload_size{};
        read_block(input_fd,file_path,file_size,block_id,buffer,offset,payload_size);
        const auto packet=srcast::encode_data(transfer_id,section_id,block_id,offset,buffer.data(),
                                              payload_size,srcast::crc32(buffer.data(),payload_size));
        if(targets.size()>=kMulticastRepairThreshold)
        {
            send_udp_packet(udp_fd,multicast_destination,packet);
            multicast_packets++;
        }else
        {
            const auto receiver_index=targets.front();
            send_udp_packet(udp_fd,receivers[receiver_index].udp_destination,packet);
            unicast_packets++;
        }

        if(pace_us>0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(pace_us));
        }
    }

    std::cout<<"repair round="<<round_id<<" multicast_packets="<<multicast_packets
             <<" unicast_packets="<<unicast_packets<<'\n';
    send_end_round(udp_fd,multicast_destination,receivers,transfer_id,section_id,round_id,total_blocks);
}

//初 DATA的故障注入只给smoke，实际环境变量为空
bool all_receivers_completed(const std::vector<ReceiverConnection>&receivers);

DeliveryResult distribute_section(int udp_fd,const sockaddr_in &multicast_destination,
    std::vector<ReceiverConnection>&receivers,int input_fd,const std::string &file_path,
    std::uint64_t file_size,std::uint64_t transfer_id,std::uint32_t total_blocks,
    const std::array<std::uint8_t,srcast::kSha256Size>&digest,std::uint32_t section_id,
    int pace_us,int max_repair_rounds,std::uint32_t section_block_limit)
{
    std::array<std::uint8_t,srcast::kPayloadSize>buffer{};
    const auto first_block=srcast::section_first_block(section_id,section_block_limit);
    const auto section_blocks=srcast::section_block_count(total_blocks,section_id,section_block_limit);
    const auto backfill_after_rounds=backfill_rounds(max_repair_rounds);
    const auto duplicate_blocks=parse_block_set_env("SRCAST_DUPLICATE_DATA_BLOCKS");
    const auto corrupt_blocks=parse_block_set_env("SRCAST_CORRUPT_DATA_BLOCKS");
    const bool reverse_initial_data=env_flag_enabled("SRCAST_REVERSE_INITIAL_DATA");
    std::cout<<"sending section="<<section_id<<" blocks="<<section_blocks<<'\n';
    if(reverse_initial_data)std::cout<<"fault injection reversed DATA section="<<section_id<<'\n';

    std::vector<std::uint32_t>local_blocks;
    local_blocks.reserve(section_blocks);
    for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
    local_blocks.push_back(local_block);
    if(reverse_initial_data)std::reverse(local_blocks.begin(),local_blocks.end());

    for(const auto local_block:local_blocks)
    {
        const auto block_id=first_block+local_block;
        std::uint64_t offset{};
        std::uint16_t payload_size{};
        read_block(input_fd,file_path,file_size,block_id,buffer,offset,payload_size);
        const auto packet=srcast::encode_data(transfer_id,section_id,block_id,offset,buffer.data(),
                   payload_size,srcast::crc32(buffer.data(),payload_size));
        if(corrupt_blocks.find(block_id)!=corrupt_blocks.end()&&!packet.empty())
        {
            auto corrupted=packet;
            corrupted.back()^=0xffU;
            send_udp_packet(udp_fd,multicast_destination,corrupted);
            std::cout<<"fault injection corrupted DATA block_id="<<block_id<<'\n';
        }else send_udp_packet(udp_fd,multicast_destination,packet);
        if(duplicate_blocks.find(block_id)!=duplicate_blocks.end())
        {
            send_udp_packet(udp_fd,multicast_destination,packet);
            std::cout<<"fault injection duplicated DATA block_id="<<block_id<<'\n';
        }

        if(pace_us>0)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(pace_us));
        }
    }

    send_end_round(udp_fd,multicast_destination,receivers,transfer_id,section_id,0,total_blocks);
    std::cout<<"section="<<section_id<<" initial multicast finished; collecting round 0 reports\n";
    collect_round_reports(receivers,transfer_id,section_id,0,
                          file_size,total_blocks,section_block_limit,digest);
    if(receiver_reported_sha_mismatch(receivers))return DeliveryResult::Sha256Mismatch;
    isolate_slow_receivers(receivers,total_blocks,section_id,0,section_block_limit,
                           backfill_after_rounds);
    for(int repair_round=1;repair_round<=max_repair_rounds&&
                           !all_receivers_completed(receivers);repair_round++)
    {
        const bool section_done=std::all_of(receivers.begin(),receivers.end(),
            [](const ReceiverConnection& receiver)
            {
                return receiver.completed||receiver.isolated||receiver.abandoned||
                      (receiver.status&&receiver.status->status==srcast::SectionStatusCode::Complete);
            });
        if(section_done)return DeliveryResult::Success;

        send_repair_round(udp_fd,multicast_destination,receivers,input_fd,file_path,file_size,
                          transfer_id,total_blocks,section_id,static_cast<std::uint32_t>(repair_round),
                          pace_us,section_block_limit);
        collect_round_reports(receivers,transfer_id,section_id,static_cast<std::uint32_t>(repair_round),
                              file_size,total_blocks,section_block_limit,digest);
        if(receiver_reported_sha_mismatch(receivers))return DeliveryResult::Sha256Mismatch;
        isolate_slow_receivers(receivers,total_blocks,section_id,
            static_cast<std::uint32_t>(repair_round),section_block_limit,backfill_after_rounds);
    }

    const bool section_delivered=std::all_of(receivers.begin(),receivers.end(),
        [](const ReceiverConnection &receiver)
        {
            return receiver.completed||receiver.isolated||receiver.abandoned||(receiver.status&&
                   receiver.status->status==srcast::SectionStatusCode::Complete);
        });
    return section_delivered?DeliveryResult::Success:DeliveryResult::Failed;
}

bool all_receivers_completed(const std::vector<ReceiverConnection>&receivers)
{
    return std::all_of(receivers.begin(),receivers.end(),
    [](const ReceiverConnection &receiver){return receiver.completed||receiver.abandoned;});
}

//文件间屏障，防止下一个FILE_META和上一个收尾串在一起
void wait_for_receivers_ready(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id)
{
    for(auto &receiver:receivers)receiver.ready_for_next_transfer=false;

    const auto deadline=SteadyClock::now()+kReadyTimeout;
    for(;;)
    {
        const bool all_ready=std::all_of(receivers.begin(),receivers.end(),
            [](const ReceiverConnection& receiver)
            {return receiver.abandoned||receiver.ready_for_next_transfer;});
        if(all_ready)
        {
            std::cout<<"all receivers are ready for the next transfer\n";
            return;
        }

        const auto now=SteadyClock::now();
        if(now>=deadline)
        {
            throw std::runtime_error("timed out waiting for receivers to reset transfer state");
        }

        const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(deadline-now);
        const auto timeout_ms=static_cast<int>(std::max<std::int64_t>(1,remaining.count()));
        const auto ready_indexes=wait_for_receiver_events(receivers,
            [](const ReceiverConnection& receiver)
            {return !receiver.abandoned&&!receiver.ready_for_next_transfer;},
            timeout_ms,"receiver ready acknowledgements");
        if(ready_indexes.empty())continue;

        for(const auto receiver_index:ready_indexes)
        {
            auto &receiver=receivers[receiver_index];
            const auto frame=receive_receiver_frame(receiver,"receiver ready acknowledgements");
            if(!frame)continue;
            if(srcast::peek_control_type(*frame)!=srcast::ControlType::ReceiverReady)
            {
                std::cerr<<"ignore unexpected message while waiting for ready"
                         <<" receiver="<<receiver.receiver_endpoint<<'\n';
                continue;
            }

            const auto message=srcast::decode_receiver_ready(*frame);
            if(message.previous_transfer_id!=transfer_id)
            {
                std::cerr<<"ignore stale RECEIVER_READY receiver="<<receiver.receiver_endpoint<<'\n';
                continue;
            }

            receiver.ready_for_next_transfer=true;
            receiver.recovery_state=srcast::ReceiverState::Idle;
            receiver.recovery_transfer_id=0;
            receiver.active_meta.reset();
            std::cout<<"receiver ready receiver="<<receiver.receiver_endpoint<<'\n';
        }
    }
}

void notify_failed_receivers(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id)
{
    const auto frame=srcast::encode_transfer_result({transfer_id,srcast::TransferResultCode::Failed});
    for(auto &receiver:receivers)
    if(!receiver.completed&&!receiver.abandoned)
    send_receiver_frame(receiver,frame,"TRANSFER_RESULT");
}

//慢receiver最后TCP补齐，避免拖住UDP主组
void backfill_one_receiver(ReceiverConnection &receiver,int input_fd,const std::string &file_path,
                           std::uint64_t file_size,std::uint64_t transfer_id,std::uint32_t total_blocks,
                           const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    std::array<std::uint8_t,srcast::kPayloadSize>buffer{};
    if(!receiver.isolated||receiver.completed||receiver.abandoned)return;
    const auto abandon_receiver=[&](const char*reason)
    {
        std::lock_guard<std::mutex>lock(output_mutex);
        std::cerr<<reason<<"; abandoning receiver="<<receiver.receiver_endpoint<<'\n';
        receiver.abandoned=true;
        receiver.control_fd.reset(-1);
    };
    {
        std::lock_guard<std::mutex>lock(output_mutex);
        std::cout<<"TCP backfill receiver="<<receiver.receiver_endpoint
                 <<" blocks="<<receiver.backfill_blocks.size()<<'\n';
    }
        if(!send_receiver_frame(receiver,srcast::encode_backfill_begin({transfer_id,total_blocks}),
                                "TCP backfill begin"))
        {
            abandon_receiver("TCP backfill connection failed");
            return;
        }
        for(const auto block_id:receiver.backfill_blocks)
        {
            std::uint64_t offset{};
            std::uint16_t payload_size{};
            read_block(input_fd,file_path,file_size,block_id,buffer,offset,payload_size);
            srcast::BackfillDataMessage data;
            data.transfer_id=transfer_id;
            data.block_id=block_id;
            data.offset=offset;
            data.payload_size=payload_size;
            data.crc32=srcast::crc32(buffer.data(),payload_size);
            data.payload.assign(buffer.begin(),buffer.begin()+payload_size);
            if(!send_receiver_frame(receiver,srcast::encode_backfill_data(data),"TCP backfill data"))
            {
                abandon_receiver("TCP backfill connection failed");
                return;
            }
        }

        if(!send_receiver_frame(receiver,srcast::encode_backfill_end
                               ({transfer_id,file_size,total_blocks,digest}),"TCP backfill end"))
        {
            abandon_receiver("TCP backfill connection failed");
            return;
        }
        const auto deadline=SteadyClock::now()+kBackfillTimeout;
        for(;;)
        {
            const auto now=SteadyClock::now();
            if(now>=deadline)
            {
                abandon_receiver("TCP backfill timed out");
                return;
            }

            const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(deadline-now);
            pollfd event{};
            event.fd=receiver.control_fd.get();
            event.events=POLLIN;
            const auto ready=::poll(&event,1,
                static_cast<int>(std::max<std::int64_t>(1,remaining.count())));
            if(ready<0)
            {
                if(errno==EINTR)continue;
                system_error("poll TCP backfill result");
            }
            if(ready==0)
            {
                abandon_receiver("TCP backfill timed out");
                return;
            }
            if((event.revents&(POLLERR|POLLHUP|POLLNVAL))!=0)
            {
                abandon_receiver("receiver control connection failed during TCP backfill");
                return;
            }
            if((event.revents&POLLIN)==0)continue;

            const auto complete_frame=receive_receiver_frame(receiver,"TCP backfill result");
            if(!complete_frame)
            {
                abandon_receiver("TCP backfill connection failed");
                return;
            }
            const auto type=srcast::peek_control_type(*complete_frame);
            if(type==srcast::ControlType::SectionStatus)
            {
                const auto status=srcast::decode_section_status(*complete_frame);
                if(status.transfer_id==transfer_id&&status.status==srcast::SectionStatusCode::Failed)
                {
                    receiver.status=status;
                    receiver.status_received=true;
                    std::lock_guard<std::mutex>lock(output_mutex);
                    std::cerr<<"TCP backfill failed receiver="<<receiver.receiver_endpoint
                             <<" status=FAILED\n";
                    return;
                }
                std::lock_guard<std::mutex>lock(output_mutex);
                std::cout<<"ignore stale SECTION_STATUS during TCP backfill"
                         <<" receiver="<<receiver.receiver_endpoint<<'\n';
                continue;
            }
            if(type!=srcast::ControlType::ReceiverComplete)
            {
                std::lock_guard<std::mutex>lock(output_mutex);
                std::cerr<<"ignore unexpected message during TCP backfill receiver="
                         <<receiver.receiver_endpoint<<'\n';
                continue;
            }

            const auto complete=srcast::decode_receiver_complete(*complete_frame);
            if(complete.transfer_id!=transfer_id||complete.file_size!=file_size||
               complete.sha256!=digest)
            {
                std::lock_guard<std::mutex>lock(output_mutex);
                std::cerr<<"ignore invalid RECEIVER_COMPLETE during TCP backfill receiver="
                         <<receiver.receiver_endpoint<<'\n';
                continue;
            }
            break;
        }

        receiver.complete_received=true;
        receiver.completed=true;
        receiver.isolated=false;
        receiver.recovery_state=srcast::ReceiverState::Completed;
        receiver.recovery_transfer_id=transfer_id;
        send_receiver_frame(receiver,srcast::encode_complete_ack
                           ({transfer_id}),"TCP backfill COMPLETE_ACK");
    {
        std::lock_guard<std::mutex>lock(output_mutex);
        std::cout<<"TCP backfill completed receiver="<<receiver.receiver_endpoint<<'\n';
    }
}

void backfill_isolated_receivers(std::vector<ReceiverConnection>&receivers,int input_fd,
    const std::string &file_path,std::uint64_t file_size,std::uint64_t transfer_id,
    std::uint32_t total_blocks,const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    std::size_t isolated_count=0;
    for(const auto &receiver:receivers)
    if(receiver.isolated&&!receiver.completed&&!receiver.abandoned)isolated_count++;
    if(isolated_count==0)return;

    ThreadPool pool(std::min(kBackfillThreadCount,isolated_count));
    std::vector<std::future<void>>tasks;
    tasks.reserve(isolated_count);
    for(auto &receiver:receivers)
    {
        if(!receiver.isolated||receiver.completed||receiver.abandoned)continue;
        tasks.push_back(pool.add(backfill_one_receiver,std::ref(receiver),input_fd,std::cref(file_path),
            file_size,transfer_id,total_blocks,std::cref(digest)));
    }
    for(auto &task:tasks)task.get();
    accept_pending_receiver_reconnects(receivers);
}

//中心每个section都等这个 ACK，确认proxy已经落盘
void send_central_status(int central_fd,std::uint64_t transfer_id,srcast::CentralStatusCode status,
    std::uint64_t file_size,const std::array<std::uint8_t,srcast::kSha256Size>&sha256)
{
    send_control_frame(central_fd,srcast::encode_central_status({transfer_id,status,file_size,sha256}));
}

void wait_for_meta_ready(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id);

void wait_for_receivers_ready(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id);

void begin_receiver_transfer(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id,
    std::uint64_t file_size,std::uint32_t total_blocks,std::uint32_t section_block_limit,
    const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    accept_pending_receiver_reconnects(receivers);
    reset_transfer_state(receivers);
    srcast::FileMetaMessage file_meta;
    file_meta.transfer_id=transfer_id;
    file_meta.section_id=kSectionId;
    file_meta.file_size=file_size;
    file_meta.block_size=srcast::kPayloadSize;
    file_meta.total_blocks=total_blocks;
    file_meta.section_block_count=section_block_limit;
    file_meta.sha256=digest;
    const auto file_meta_frame=srcast::encode_file_meta(file_meta);
    for(auto &receiver:receivers)
    {
        receiver.active_meta=file_meta;
        if(receiver.recovery_state==srcast::ReceiverState::Completed&&
           receiver.recovery_transfer_id==transfer_id)
        {
            receiver.meta_ready=true;
            receiver.status_received=true;
            receiver.complete_received=true;
            receiver.completed=true;
            send_receiver_frame(receiver,srcast::encode_complete_ack
                               ({transfer_id}),"recovered COMPLETE_ACK");
            continue;
        }

        if(receiver.recovery_state==srcast::ReceiverState::Completed)
        {
            send_receiver_frame(receiver,srcast::encode_complete_ack
                               ({receiver.recovery_transfer_id}),
                                "previous COMPLETE_ACK");
        }else if(receiver.recovery_state==srcast::ReceiverState::Receiving&&
                 receiver.recovery_transfer_id!=transfer_id)
        {
            send_receiver_frame(receiver,srcast::encode_transfer_result
                               ({receiver.recovery_transfer_id,srcast::TransferResultCode::Failed}),
                                "previous transfer result");
        }
        receiver.recovery_state=srcast::ReceiverState::Receiving;
        receiver.recovery_transfer_id=transfer_id;
        send_receiver_frame(receiver,file_meta_frame,"FILE_META");
    }
    wait_for_meta_ready(receivers,transfer_id);
}

void finish_failed_transfer(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id)
{
    notify_failed_receivers(receivers,transfer_id);
    wait_for_receivers_ready(receivers,transfer_id);
}

DeliveryResult send_file_attempt(int udp_fd,const sockaddr_in &multicast_destination,
    std::vector<ReceiverConnection>&receivers,int input_fd,const std::string &file_path,
    std::uint64_t file_size,std::uint64_t transfer_id,std::uint32_t total_blocks,
    const std::array<std::uint8_t,srcast::kSha256Size>&digest,int pace_us,int max_repair_rounds,
    std::uint32_t section_block_limit)
{
    begin_receiver_transfer(receivers,transfer_id,file_size,total_blocks,section_block_limit,digest);
    DeliveryResult result=DeliveryResult::Success;
    const auto section_count=srcast::section_count_for_blocks(total_blocks,section_block_limit);
    for(std::uint32_t section_id=0;section_id<section_count&&!all_receivers_completed(receivers);
                                   section_id++)
    {
        result=distribute_section(udp_fd,multicast_destination,receivers,input_fd,file_path,file_size,
            transfer_id,total_blocks,digest,section_id,pace_us,max_repair_rounds,section_block_limit);
        if(result!=DeliveryResult::Success)break;
    }

    if(result!=DeliveryResult::Sha256Mismatch)
    {
        backfill_isolated_receivers(receivers,input_fd,file_path,file_size,
                                    transfer_id,total_blocks,digest);
        if(receiver_reported_sha_mismatch(receivers))result=DeliveryResult::Sha256Mismatch;
    }
    if(all_receivers_completed(receivers))result=DeliveryResult::Success;
    else if(result==DeliveryResult::Success)result=DeliveryResult::Failed;

    if(result!=DeliveryResult::Success)notify_failed_receivers(receivers,transfer_id);
    wait_for_receivers_ready(receivers,transfer_id);
    return result;
}

std::optional<CachedCentralFile>receive_cached_file_from_central(int central_fd,
    const std::string &cache_directory,int udp_fd,const sockaddr_in &multicast_destination,
    std::vector<ReceiverConnection>& receivers,int pace_us,int max_repair_rounds)
{
//中心可能发新文件，也可能只发 SESSION_END
    const auto first_frame=receive_control_frame(central_fd);
    const auto first_type=srcast::peek_control_type(first_frame);
    if(first_type==srcast::ControlType::CentralSessionEnd)
    {
        srcast::decode_central_session_end(first_frame);
        return std::nullopt;
    }
    if(first_type!=srcast::ControlType::CentralFileMeta)
    {
        throw std::runtime_error("expected CENTRAL_FILE_META from central sender");
    }

    const auto meta=srcast::decode_central_file_meta(first_frame);
    srcast::validate_section_block_count(meta.section_block_count);
    if(meta.block_size!=srcast::kPayloadSize)
    {
        throw std::runtime_error("unsupported central block size");
    }

    const auto expected_blocks64=(meta.file_size+meta.block_size-1)/meta.block_size;
    if(expected_blocks64>std::numeric_limits<std::uint32_t>::max()||
       static_cast<std::uint32_t>(expected_blocks64)!=meta.total_blocks)
    {
        throw std::runtime_error("inconsistent central file block count");
    }

    std::error_code directory_error;
    std::filesystem::create_directories(cache_directory,directory_error);
    if(directory_error)
    {
        throw std::runtime_error("create central cache directory failed: "+directory_error.message());
    }
    const auto final_path=(std::filesystem::path(cache_directory)/
                          ("central-transfer-"+std::to_string(meta.transfer_id)+".bin")).string();
    const auto temporary_path=final_path+".part";
    if(std::filesystem::exists(final_path))
    {
        const auto actual_digest=srcast::sha256_file(final_path);
        if(actual_digest==meta.sha256)
        {
            accept_pending_receiver_reconnects(receivers);
            srcast::FileMetaMessage file_meta;
            file_meta.transfer_id=meta.transfer_id;
            file_meta.section_id=kSectionId;
            file_meta.file_size=meta.file_size;
            file_meta.block_size=meta.block_size;
            file_meta.total_blocks=meta.total_blocks;
            file_meta.section_block_count=meta.section_block_count;
            file_meta.sha256=meta.sha256;
            for(auto &receiver:receivers)
            {
                receiver.active_meta=file_meta;
                if(receiver.recovery_state!=srcast::ReceiverState::Completed||
                   receiver.recovery_transfer_id!=meta.transfer_id)continue;
                receiver.meta_ready=true;
                receiver.complete_received=true;
                receiver.completed=true;
                send_receiver_frame(receiver,srcast::encode_complete_ack
                                   ({meta.transfer_id}),
                                    "cached transfer COMPLETE_ACK");
            }
            const auto section_count=srcast::section_count_for_blocks(meta.total_blocks,
                                                                      meta.section_block_count);
            send_control_frame(central_fd,srcast::encode_central_resume({meta.transfer_id,section_count,
                            section_count,meta.file_size,actual_digest}));
            std::cout<<"central transfer already cached path="<<final_path<<'\n';
            return CachedCentralFile{final_path,meta.transfer_id,
                                     meta.file_size,meta.total_blocks,actual_digest};
        }
    }
    std::uint64_t existing_size=0;
    std::error_code existing_error;
    if(std::filesystem::exists(temporary_path,existing_error))
    existing_size=std::filesystem::file_size(temporary_path,existing_error);
    if(existing_error)
    {
        throw std::runtime_error("stat central cache temporary file failed: "+existing_error.message());
    }
    const auto additional=existing_size<meta.file_size?meta.file_size-existing_size:0;
    require_storage_limit(cache_directory,additional,
                         "SRCAST_CACHE_LIMIT_BYTES","central cache directory");
    require_disk_space(cache_directory,meta.file_size,"central cache directory");
    const auto checkpoint_path=central_checkpoint_path(final_path);
    const auto section_count=srcast::
                             section_count_for_blocks(meta.total_blocks,meta.section_block_count);
    const auto resume_section=load_central_checkpoint(checkpoint_path,temporary_path,meta);
//断点恢复时复用 .part；新任务才截断
    FileDescriptor output(::open(temporary_path.c_str(),O_CREAT|(resume_section==0?O_TRUNC:0)|
                                 O_RDWR|O_CLOEXEC,0644));
    if(output.get()<0)
    {
        system_error("open central cache temporary file");
    }
    if(::ftruncate(output.get(),static_cast<off_t>(meta.file_size))!=0)
    {
        system_error("ftruncate central cache");
    }

    std::vector<std::uint8_t>received(meta.total_blocks,0);
    for(std::uint32_t section_id=0;section_id<resume_section;section_id++)
    {
        const auto first_block=srcast::section_first_block(section_id,meta.section_block_count);
        const auto section_blocks=srcast::section_block_count(meta.total_blocks,
                                                              section_id,meta.section_block_count);
        for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
        received[first_block+local_block]=1;
    }
    std::uint32_t received_count=std::min<std::uint32_t>(meta.total_blocks,srcast::section_first_block(
                                                         resume_section,meta.section_block_count));
    std::uint64_t duplicate_count=0;
    std::uint64_t rejected_count=0;
    std::cout<<"central transfer started transfer_id="<<meta.transfer_id<<" size="<<meta.file_size
             <<" blocks="<<meta.total_blocks<<" resume_section="
             <<resume_section<<'/'<<section_count<<'\n';
    begin_receiver_transfer(receivers,meta.transfer_id,meta.file_size,meta.total_blocks,
                            meta.section_block_count,meta.sha256);
    send_control_frame(central_fd,srcast::encode_central_resume({meta.transfer_id,resume_section,
                       section_count,meta.file_size,std::array<std::uint8_t,srcast::kSha256Size>{}}));
//之后只接收resume_section及后面的DATA
    auto fail_after_meta=[&](const std::string &reason,
                             const std::array<std::uint8_t,srcast::kSha256Size>&sha256)
    {
        send_central_status(central_fd,meta.transfer_id,srcast::CentralStatusCode::Failed,
                            meta.file_size,sha256);
        notify_failed_receivers(receivers,meta.transfer_id);
        wait_for_receivers_ready(receivers,meta.transfer_id);
        throw std::runtime_error(reason);
    };

    std::uint32_t next_section_id=resume_section;
    for(;;)
    {
        const auto frame=receive_control_frame(central_fd);
        const auto type=srcast::peek_control_type(frame);
        if(type==srcast::ControlType::CentralData)
        {
            const auto data=srcast::decode_central_data(frame);
            if(data.transfer_id!=meta.transfer_id||data.block_id>=meta.total_blocks||
               data.section_id!=next_section_id||data.section_id!=
               srcast::section_id_for_block(data.block_id,meta.section_block_count))
            {
                rejected_count++;
                continue;
            }

            const auto expected_offset=static_cast<std::uint64_t>(data.block_id)*meta.block_size;
            const auto expected_size=static_cast<std::uint16_t>(std::min<std::uint64_t>
                                        (meta.block_size,meta.file_size-expected_offset));
            if(data.offset!=expected_offset||data.payload_size!=expected_size||
               data.offset+data.payload_size>meta.file_size||
               srcast::crc32(data.payload.data(),data.payload.size())!=data.crc32)
            {
                rejected_count++;
                continue;
            }
            if(received[data.block_id]!=0)
            {
                duplicate_count++;
                continue;
            }

            write_all_at(output.get(),data.payload.data(),data.payload.size(),data.offset);
            received[data.block_id]=1;
            received_count++;
            if(received_count%1000U==0U||received_count==meta.total_blocks)
            std::cout<<"central cached "<<received_count<<'/'<<meta.total_blocks<<" blocks\n";
            continue;
        }

        if(type!=srcast::ControlType::CentralFileEnd)
        {
            throw std::runtime_error("unexpected central control message during file transfer");
        }

//中心声明section 结束后，proxy才检查这一段有没有收齐
        const auto end=srcast::decode_central_file_end(frame);
        if(end.transfer_id!=meta.transfer_id||end.total_blocks!=meta.total_blocks||
           end.section_id!=next_section_id||end.section_id>=section_count||rejected_count!=0)
        {
            fail_after_meta("central transfer did not complete cleanly",
                            std::array<std::uint8_t,srcast::kSha256Size>{});
        }

        const auto first_block=srcast::section_first_block(end.section_id,meta.section_block_count);
        const auto section_blocks=srcast::section_block_count(meta.total_blocks,
                                                              end.section_id,meta.section_block_count);
        for(std::uint32_t local_block=0;local_block<section_blocks;local_block++)
        {
            if(received[first_block+local_block]==0)
                fail_after_meta("central section has missing blocks",
                std::array<std::uint8_t,srcast::kSha256Size>{});
        }

//proxy先本地分发已缓存section，再向中心ACK
        const auto section_result=distribute_section(udp_fd,multicast_destination,receivers,
            output.get(),temporary_path,meta.file_size,meta.transfer_id,meta.total_blocks,meta.sha256,
            end.section_id,pace_us,max_repair_rounds,meta.section_block_count);
        const bool final_section=end.section_id+1U==section_count;
        if(section_result==DeliveryResult::Failed||
           (section_result==DeliveryResult::Sha256Mismatch&&!final_section))
        fail_after_meta("receiver repair failed for central section",
                        std::array<std::uint8_t,srcast::kSha256Size>{});

        if(!final_section)
        {
            const auto completed_section=end.section_id+1U;
            if(::fsync(output.get())!=0)
            {
                system_error("fsync central cache section");
            }
//非最终section只记checkpoint，不SHA
            save_central_checkpoint(checkpoint_path,meta,completed_section);
            send_central_status(central_fd,meta.transfer_id,srcast::CentralStatusCode::Cached,
                                meta.file_size,std::array<std::uint8_t,srcast::kSha256Size>{});
            std::cout<<"central section cached section="<<end.section_id<<'\n';
            next_section_id=completed_section;
            continue;
        }

        if(section_result==DeliveryResult::Sha256Mismatch)
        finish_failed_transfer(receivers,meta.transfer_id);

        if(received_count!=meta.total_blocks)
        fail_after_meta("central transfer missing final blocks",
        std::array<std::uint8_t,srcast::kSha256Size>{});

        if(::fsync(output.get())!=0)
        {
            system_error("fsync central cache");
        }
        output.reset(-1);
//最后一段收完后文件校验，再rename成正式缓存
        const auto actual_digest=srcast::sha256_file(temporary_path);
        if(actual_digest!=meta.sha256)
        fail_after_meta("central cached file SHA-256 mismatch",actual_digest);

        std::error_code rename_error;
        std::filesystem::rename(temporary_path,final_path,rename_error);
        if(rename_error)
        {
            throw std::runtime_error("commit central cache failed: "+rename_error.message());
        }

        std::error_code remove_error;
        std::filesystem::remove(checkpoint_path,remove_error);
//正式缓存可读后，再给慢receiver TCP backfill
        FileDescriptor backfill_input(::open(final_path.c_str(),O_RDONLY|O_CLOEXEC));
        if(backfill_input.get()<0)
        {
            system_error("open central cache for TCP backfill");
        }
        auto delivery_result=section_result;
        if(delivery_result==DeliveryResult::Success)
        {
            backfill_isolated_receivers(receivers,backfill_input.get(),final_path,meta.file_size,
                                        meta.transfer_id,meta.total_blocks,actual_digest);
            if(receiver_reported_sha_mismatch(receivers))
            {
                delivery_result=DeliveryResult::Sha256Mismatch;
                finish_failed_transfer(receivers,meta.transfer_id);
            }else if(!all_receivers_completed(receivers))
            {
                delivery_result=DeliveryResult::Failed;
                finish_failed_transfer(receivers,meta.transfer_id);
            }
        }

        std::unordered_set<std::uint64_t>used_transfer_ids{meta.transfer_id};
        for(int retry=0;delivery_result==DeliveryResult::Sha256Mismatch&&
                        retry<kMaxTransferRetries;retry++)
        {
            const auto retry_transfer_id=create_transfer_id(used_transfer_ids);
            used_transfer_ids.insert(retry_transfer_id);
            delivery_result=send_file_attempt(udp_fd,multicast_destination,receivers,
                backfill_input.get(),final_path,meta.file_size,retry_transfer_id,meta.total_blocks,
                actual_digest,pace_us,max_repair_rounds,meta.section_block_count);
        }
        if(delivery_result!=DeliveryResult::Success)
        {
            const auto completed_count=static_cast<std::size_t>(std::count_if
                (receivers.begin(),receivers.end(),[](const ReceiverConnection &receiver)
                {return receiver.completed;}));
            std::cout<<"file finished PARTIAL completed="<<completed_count<<'/'<<receivers.size()
                     <<" after transfer retries\n";
            abandon_incomplete_receivers(receivers);
        }

        send_central_status(central_fd,meta.transfer_id,srcast::CentralStatusCode::Cached,
                            meta.file_size,actual_digest);
        if(delivery_result==DeliveryResult::Success&&section_result==DeliveryResult::Success&&
           used_transfer_ids.size()==1U)
        wait_for_receivers_ready(receivers,meta.transfer_id);
        if(delivery_result==DeliveryResult::Success)
        std::cout<<"file completed by all receivers: "<<final_path<<'\n';
        std::cout<<"central transfer cached path="<<final_path<<" duplicates="<<duplicate_count
                 <<" rejected="<<rejected_count<<'\n';
        return CachedCentralFile{final_path,meta.transfer_id,meta.file_size,
                                 meta.total_blocks,actual_digest};
    }
}

void wait_for_meta_ready(std::vector<ReceiverConnection>&receivers,std::uint64_t transfer_id)
{
    const auto deadline=SteadyClock::now()+kMetaReadyTimeout;
    for(;;)
    {
        const bool all_ready=std::all_of(receivers.begin(),receivers.end(),
            [](const ReceiverConnection &receiver)
            {return receiver.completed||receiver.abandoned||receiver.meta_ready;});
        if(all_ready)
        {
            std::cout<<"all receivers accepted FILE_META;"<<"starting UDP DATA\n";
            return;
        }

        const auto now=SteadyClock::now();
        if(now>=deadline)
        {
            throw std::runtime_error("timed out waiting for META_READY");
        }

        const auto remaining=std::chrono::duration_cast<std::chrono::milliseconds>(deadline-now);
        const auto timeout_ms=static_cast<int>(std::max<std::int64_t>(1,remaining.count()));
        const auto ready_indexes=wait_for_receiver_events(receivers,
            [](const ReceiverConnection &receiver)
            {return !receiver.completed&&!receiver.abandoned&&!receiver.meta_ready;},
            timeout_ms,"META_READY");
        if(ready_indexes.empty())continue;

        for(const auto receiver_index:ready_indexes)
        {
            auto &receiver=receivers[receiver_index];
//receive_control_frame会读4字节+完整消息体
            const auto frame=receive_receiver_frame(receiver,"META_READY");
            if(!frame)continue;
            const auto type=srcast::peek_control_type(*frame);
            if(type==srcast::ControlType::ReceiverReady)
            {
                const auto message=srcast::decode_receiver_ready(*frame);
                if(message.previous_transfer_id==transfer_id)
                std::cout<<"ignore previous RECEIVER_READY receiver="<<receiver.receiver_endpoint<<'\n';
                continue;
            }
            if(type==srcast::ControlType::ReceiverComplete)
            {
                const auto message=srcast::decode_receiver_complete(*frame);
                if(message.transfer_id==transfer_id&&receiver.active_meta&&
                   message.file_size==receiver.active_meta->file_size&&
                   message.sha256==receiver.active_meta->sha256)
                {
                    receiver.meta_ready=true;
                    receiver.complete_received=true;
                    receiver.completed=true;
                    receiver.recovery_state=srcast::ReceiverState::Completed;
                    receiver.recovery_transfer_id=transfer_id;
                    send_receiver_frame(receiver,srcast::encode_complete_ack
                                       ({transfer_id}),"recovered COMPLETE_ACK");
                }
                continue;
            }
            if(type!=srcast::ControlType::MetaReady)
            {
                std::cerr<<"ignore unexpected control message while waiting for META_READY"
                         <<" receiver="<<receiver.receiver_endpoint<<'\n';
                continue;
            }

            const auto message=srcast::decode_meta_ready(*frame);
            if(message.transfer_id!=transfer_id)
            {
                throw std::runtime_error("invalid or stale META_READY");
            }

            receiver.meta_ready=true;
            receiver.recovery_state=srcast::ReceiverState::Receiving;
            receiver.recovery_transfer_id=transfer_id;
            std::cout<<"META_READY receiver="<<receiver.receiver_endpoint<<'\n';
        }
    }
}

//单文件流程 FILE_META 组播 修复 backfill READY
void send_file(int udp_fd,const sockaddr_in &multicast_destination,
    std::vector<ReceiverConnection>&receivers,const std::string &file_path,int pace_us,
    int max_repair_rounds,std::uint32_t section_block_limit)
{
    struct stat status{};
    if(::stat(file_path.c_str(),&status)!=0)
    {
        system_error("stat "+file_path);
    }
    if(!S_ISREG(status.st_mode)||status.st_size<0)
    {
        throw std::runtime_error("source path must be a regular file: "+file_path);
    }

    const auto file_size=static_cast<std::uint64_t>(status.st_size);
    const auto total_blocks64=(file_size+srcast::kPayloadSize-1)/srcast::kPayloadSize;
    if(total_blocks64>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error("file is too large for protocol block numbering: "+file_path);
    }

    const auto total_blocks=static_cast<std::uint32_t>(total_blocks64);
    const auto digest=srcast::sha256_file(file_path);
    FileDescriptor input(::open(file_path.c_str(),O_RDONLY|O_CLOEXEC));
    if(input.get()<0)
    {
        system_error("open source file "+file_path);
    }

    std::unordered_set<std::uint64_t>used_transfer_ids;
    DeliveryResult result=DeliveryResult::Failed;
    for(int attempt=0;attempt<=kMaxTransferRetries;attempt++)
    {
        const auto transfer_id=create_transfer_id(used_transfer_ids);
        used_transfer_ids.insert(transfer_id);
        result=send_file_attempt(udp_fd,multicast_destination,receivers,input.get(),file_path,
            file_size,transfer_id,total_blocks,digest,pace_us,max_repair_rounds,section_block_limit);
        if(result!=DeliveryResult::Sha256Mismatch)break;
    }

    if(result==DeliveryResult::Success)std::cout<<"file completed by all receivers:"<<file_path<<'\n';
    else
    {
        const auto completed_count=static_cast<std::size_t>(std::count_if
            (receivers.begin(),receivers.end(),
            [](const ReceiverConnection& receiver){return receiver.completed;}));
        std::cout<<"file finished PARTIAL completed="<<completed_count<<'/'<<receivers.size()
                 <<" after max repair rounds\n";
        abandon_incomplete_receivers(receivers);
    }
}
}

int main(int argc,char**argv) try
{
    const auto options=parse_options(argc,argv);
    const auto &multicast_ip=options.multicast_ip;//组播地址
    const auto udp_port=options.udp_port;//udp端口
    const auto &interface_ip=options.interface_ip;//本机网卡ip
    const auto control_port=options.control_port;//tcp端口
    const auto receiver_count=options.receiver_count;//接收端数量
    const auto pace_us=options.pace_us;//block发送间隔
    const auto gap_ms=options.gap_ms;//文件间隔时间
    const auto max_repair_rounds=options.max_repair_rounds;//最大修复轮数
    const auto section_block_limit=options.section_block_limit;//一个section的block数量
    const auto central_mode=options.central_mode;//中心模式？不启用园区内发
    const auto central_port=options.central_port;//中心端口
    const auto &cache_directory=options.cache_directory;//要发的文件

    FileDescriptor udp_fd(::socket(AF_INET,SOCK_DGRAM|SOCK_CLOEXEC,0));
    if(udp_fd.get()<0)
    {
        system_error("socket UDP");
    }

    const unsigned char ttl=1;//路由器不转发
    const unsigned char loop=1;//允许回环
    if(::setsockopt(udp_fd.get(),IPPROTO_IP,IP_MULTICAST_TTL,&ttl,sizeof(ttl))!=0)
    {
        system_error("setsockopt IP_MULTICAST_TTL");
    }
    if(::setsockopt(udp_fd.get(),IPPROTO_IP,IP_MULTICAST_LOOP,&loop,sizeof(loop))!=0)
    {
        system_error("setsockopt IP_MULTICAST_LOOP");
    }

    if(interface_ip!="0.0.0.0")//如果指定网卡ip
    {
        in_addr interface_address{};
        if(::inet_pton(AF_INET,interface_ip.c_str(),&interface_address)!=1)
        {
            throw std::runtime_error("invalid interface IPv4 address");
        }
        if(::setsockopt(udp_fd.get(),IPPROTO_IP,IP_MULTICAST_IF,
                        &interface_address,sizeof(interface_address))!=0)
        {
            system_error("setsockopt IP_MULTICAST_IF");
        }
    }

    sockaddr_in multicast_destination{};
    multicast_destination.sin_family=AF_INET;
    multicast_destination.sin_port=htons(static_cast<std::uint16_t>(udp_port));
    if(::inet_pton(AF_INET,multicast_ip.c_str(),&multicast_destination.sin_addr)!=1||
                   !IN_MULTICAST(ntohl(multicast_destination.sin_addr.s_addr)))
    {
        throw std::runtime_error("destination must be an IPv4 multicast address");
    }

    //先开控制面，接收端注册完再接中心
    auto listener=create_control_listener(control_port);
    receiver_listener_fd=listener.get();
    std::optional<FileDescriptor>central_listener;
    if(central_mode)
    {
        central_listener.emplace(create_control_listener(central_port));
        std::cout<<"waiting for central sender on port "<<central_port<<'\n'<<std::flush;
    }

    auto receivers=accept_receivers(listener.get(),static_cast<std::size_t>(receiver_count));
    if(central_mode)
    {
        bool central_session_finished=false;
        while(!central_session_finished)
        {
            sockaddr_in central_peer{};
            socklen_t central_peer_size=sizeof(central_peer);
            int accepted=-1;
            for(;;)
            {
                accepted=::accept4(central_listener->get(),reinterpret_cast<sockaddr*>(&central_peer),
                                   &central_peer_size,SOCK_CLOEXEC);
                if(accepted>=0)break;
                if(errno==EINTR)continue;
                system_error("accept central sender");
            }

            FileDescriptor central_fd(accepted);
            std::cout<<"central sender connected\n";
            for(;;)
            {
                try
                {
                    auto cached=receive_cached_file_from_central(central_fd.get(),cache_directory,
                            udp_fd.get(),multicast_destination,receivers,pace_us,max_repair_rounds);
                    if(!cached)
                    {
                        central_session_finished=true;
                        break;
                    }
                }catch(const std::exception& error)
                {
                    std::cerr<<"central session interrupted:"<<error.what()<<"; waiting for reconnect\n";
                    break;
                }
            }
        }
    }else
    {
        for(std::size_t argument_index=0;argument_index<options.files.size();argument_index++)
        {
            send_file(udp_fd.get(),multicast_destination,receivers,options.files[argument_index],pace_us,
                      max_repair_rounds,section_block_limit);
            if(argument_index+1<options.files.size()&&gap_ms>0)
            {
                std::cout<<"waiting "<<gap_ms<<"ms before the next file\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(gap_ms));
            }
        }
    }

    const auto session_end=srcast::encode_session_end();
    accept_pending_receiver_reconnects(receivers);
    for(auto &receiver:receivers)send_receiver_frame(receiver,session_end,"SESSION_END");

    std::cout<<"all files processed serially\n";
    return 0;
}catch(const std::exception&error)
{
    std::cerr<<"sender error: "<<error.what()<<'\n';
    return 1;
}