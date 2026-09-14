#include"checksum.hpp"
#include"cli_utils.hpp"
#include"control_io.hpp"
#include"file_descriptor.hpp"
#include"file_io.hpp"
#include"protocol.hpp"
#include"transfer_options.hpp"

#include<arpa/inet.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<sys/socket.h>
#include<sys/stat.h>
#include<unistd.h>
#include<algorithm>
#include<array>
#include<cerrno>
#include<chrono>
#include<cstdint>
#include<cstdlib>
#include<cstring>
#include<iostream>
#include<limits>
#include<stdexcept>
#include<string>
#include<thread>
#include<utility>
#include<vector>

namespace
{
struct ProxyEndpoint
{
    std::string ip;
    int port{};
};

struct ProxySession
{
    ProxyEndpoint endpoint;
    FileDescriptor fd;
    bool stopped{false};
};

constexpr auto kReconnectDelay=std::chrono::milliseconds(500);//0.5s一次重连

//控制4+body
FileDescriptor connect_proxy(const std::string &proxy_ip,int central_port)
{
    FileDescriptor fd(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0));
    if(fd.get()<0)system_error("socket central");

    sockaddr_in proxy{};
    proxy.sin_family=AF_INET;
    proxy.sin_port=htons(static_cast<std::uint16_t>(central_port));
    if(::inet_pton(AF_INET,proxy_ip.c_str(),&proxy.sin_addr)!=1)
    {
        throw std::runtime_error("invalid proxy IPv4 address");
    }

    for(;;)
    {
        if(::connect(fd.get(),reinterpret_cast<sockaddr*>(&proxy),sizeof(proxy))==0)return fd;
        if(errno==EINTR)continue;
        system_error("connect proxy central port");
    }
}

//transfer_id用文件摘要生成，重试同文件时id稳定
std::uint64_t create_transfer_id(const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    std::uint64_t value=0;
    for(std::size_t index=0;index<sizeof(value);index++)value=(value<<8U)|digest[index];
    return value;
}

//测试用，确认若干section后主动断线
std::uint32_t stop_after_sections_for_test()
{
    const char *raw=std::getenv("SRCAST_STOP_AFTER_SECTIONS");
    if(raw==nullptr||*raw=='\0')return 0;

    char *end=nullptr;
    errno=0;
    const auto value=std::strtoul(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE||value>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error("invalid SRCAST_STOP_AFTER_SECTIONS value");
    }
    return static_cast<std::uint32_t>(value);
}

void usage(std::ostream &output,const char *program)
{
    output
        <<"SiteRepairCast - ROLE: CENTRAL\n\n"
        <<"Usage:\n"
        <<"  "<<program<<" --proxy <IPv4> <port> [--proxy <IPv4> <port> ...]\n"
        <<"      [--pace-us <N>] [--section-blocks <N>]\n"
        <<"      --file <path> [--file <path> ...]\n"
        <<"  "<<program<<" <proxy_ip> <central_port> <pace_us> <file1> [file2 ...]\n\n"
        <<"Required options (named mode):\n"
        <<"  --proxy <IPv4> <port>   Proxy Central TCP endpoint; repeatable.\n"
        <<"  --file <path>           Source file; repeatable.\n\n"
        <<"Optional options and defaults:\n"
        <<"  --pace-us <N>           Per-block delay, default 0.\n"
        <<"  --section-blocks <N>    Blocks per section; CLI overrides"
        <<" SRCAST_SECTION_BLOCKS, default "<<srcast::kDefaultSectionBlockCount<<".\n"
        <<"  -h, --help              Show this help.\n\n"
        <<"Examples:\n"
        <<"  "<<program<<" --proxy 192.168.1.20 7000 --pace-us 0"
        <<" --section-blocks 4096 --file ./input.bin\n"
        <<"  "<<program<<" --proxy 192.168.1.20 7000 --proxy 192.168.1.21 7000"
        <<" --file ./a.bin --file ./b.bin\n";
}

struct CentralOptions
{
    std::vector<ProxyEndpoint>endpoints;
    std::vector<std::string>files;
    int pace_us{};
    std::uint32_t section_blocks{};
};

CentralOptions parse_options(int argc,char **argv)
{
    CentralOptions options;
    options.pace_us=0;
    options.section_blocks=section_blocks_from_env();
    if(argc<2)throw std::runtime_error("missing arguments; use --help");
    const bool flag_mode=argv[1][0]=='-';

    if(flag_mode)
    {
        for(int index=1;index<argc;index++)
        {
            const std::string option=argv[index];
            if(option=="--pace-us")
            {
                if(++index>=argc)throw std::runtime_error("missing --pace-us value");
                options.pace_us=srcast::cli::parse_nonnegative_int("--pace-us",argv[index]);
                continue;
            }
            if(option=="--proxy")
            {
                if(index+2>=argc)throw std::runtime_error("missing --proxy values");
                const std::string ip=argv[++index];
                srcast::cli::parse_ipv4("--proxy",ip);
                const auto port=srcast::cli::parse_port("--proxy port",argv[++index]);
                options.endpoints.push_back({ip,port});
                continue;
            }
            if(option=="--section-blocks")
            {
                if(++index>=argc)throw std::runtime_error("missing --section-blocks value");
                options.section_blocks=srcast::cli::parse_positive_u32(
                    "--section-blocks",argv[index]);
                continue;
            }
            if(option=="--file")
            {
                if(++index>=argc)throw std::runtime_error("missing --file value");
                options.files.emplace_back(argv[index]);
                continue;
            }
            if(option.rfind("--",0)==0||option=="-h")
            {
                throw std::runtime_error("unknown option: "+option);
            }
            options.files.emplace_back(option);//兼容原有flag模式的裸文件参数
        }
    }
    else
    {
        if(argc<5)throw std::runtime_error("legacy mode requires proxy, port, pace, and file");
        const std::string ip=argv[1];
        srcast::cli::parse_ipv4("proxy_ip",ip);
        options.endpoints.push_back({ip,srcast::cli::parse_port("central_port",argv[2])});
        options.pace_us=srcast::cli::parse_nonnegative_int("pace_us",argv[3]);
        for(int index=4;index<argc;index++)options.files.emplace_back(argv[index]);
    }

    if(options.endpoints.empty())throw std::runtime_error("--proxy is required");
    if(options.files.empty())throw std::runtime_error("--file is required");
    return options;
}

//true发送这个文件成功
bool send_file(int central_fd,const std::string &file_path,int pace_us,std::uint32_t section_blocks)
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
    const auto transfer_id=create_transfer_id(digest);
    FileDescriptor input(::open(file_path.c_str(),O_RDONLY|O_CLOEXEC));
    if(input.get()<0)
    {
        system_error("open source file "+file_path);
    }

    srcast::CentralFileMetaMessage meta;
    meta.transfer_id=transfer_id;
    meta.file_size=file_size;
    meta.block_size=srcast::kPayloadSize;
    meta.total_blocks=total_blocks;
    meta.section_block_count=section_blocks;
    meta.sha256=digest;
    std::cout<<"central sending file="<<file_path<<" transfer_id="<<transfer_id
             <<" size="<<file_size<<" blocks="<<total_blocks<<'\n';

    send_control_frame(central_fd,srcast::encode_central_file_meta(meta));
    const auto section_count=srcast::section_count_for_blocks(total_blocks,section_blocks);
    //代理重启：中心按proxy返回的section继续发
    const auto resume=srcast::decode_central_resume(receive_control_frame(central_fd));

    if(resume.transfer_id!=transfer_id||resume.file_size!=file_size|| 
       resume.total_sections!=section_count||resume.next_section_id>section_count)
    {
        throw std::runtime_error("invalid CENTRAL_RESUME from proxy: "+file_path);
    }
    if(resume.next_section_id==section_count)
    {
        if(resume.sha256!=digest)
        {
            throw std::runtime_error("proxy cached digest mismatch: "+file_path);
        }
        std::cout<<"central transfer already cached transfer_id="<<transfer_id<<'\n';
        return true;
    }

    std::cout<<"central resume transfer_id="<<transfer_id<<" next_section="<<resume.next_section_id<<'/'<<section_count<<'\n';
    std::vector<std::uint8_t>payload;
    const auto stop_after_sections=stop_after_sections_for_test();
    std::uint32_t confirmed_this_run=0;
    //每个section发完都等proxy确认，同接收端一样
    for(std::uint32_t section_id=resume.next_section_id;section_id<section_count;section_id++)
    {
        const auto first_block=srcast::section_first_block(section_id,section_blocks);
        const auto section_block_count=srcast::section_block_count(total_blocks,section_id,meta.section_block_count);
        for(std::uint32_t local_block=0;local_block<section_block_count;local_block++)
        {
            const auto block_id=first_block+local_block;
            std::uint64_t offset{};
            read_block(input.get(),file_path,file_size,block_id,payload,offset);
            srcast::CentralDataMessage data;
            data.transfer_id=transfer_id;
            data.section_id=section_id;
            data.block_id=block_id;
            data.offset=offset;
            data.payload_size=static_cast<std::uint16_t>(payload.size());
            data.crc32=srcast::crc32(payload.data(),payload.size());
            data.payload=payload;
            send_control_frame(central_fd,srcast::encode_central_data(data));
            if(pace_us>0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(pace_us));
            }
        }

        send_control_frame(central_fd,srcast::encode_central_file_end({transfer_id,section_id,total_blocks}));
        //只有proxy落盘并更新checkpoint后，中心才推进下一段。
        const auto response=receive_control_frame(central_fd);
        const auto result=srcast::decode_central_status(response);
        if(result.transfer_id!=transfer_id||result.status!=srcast::CentralStatusCode::Cached||result.file_size!=file_size)
        {
            throw std::runtime_error("proxy did not confirm cached section: "+file_path);
        }
        if(section_id+1U==section_count&&result.sha256!=digest)
        {
            throw std::runtime_error("proxy final digest mismatch: "+file_path);
        }
        std::cout<<"central confirmed section="<<section_id<<" transfer_id="<<transfer_id<<'\n';
        confirmed_this_run++;
        if(stop_after_sections!=0&&confirmed_this_run>=stop_after_sections&&section_id+1U<section_count)
        {
            //测试模拟中心断线，proxy端checkpoint能接
            std::cout<<"test hook closing central connection after "<<confirmed_this_run<<" confirmed sections\n";
            return false;
        }
    }

    std::cout<<"central confirmed cached transfer_id="<<transfer_id<<'\n';
    return true;
}

void run_proxy_session(ProxySession &session,const std::vector<std::string>&files,int pace_us,std::uint32_t section_blocks)
{
    std::size_t file_index=0;
    while(file_index<files.size())
    {
        if(session.fd.get()<0)//重连
        {
            try
            {
                session.fd=connect_proxy(session.endpoint.ip,session.endpoint.port);
                std::cout<<"proxy connected "<<session.endpoint.ip<<':'<<session.endpoint.port<<'\n';
            }catch(const std::exception &error)
            {
                std::cerr<<"proxy reconnect failed "<<session.endpoint.ip<<':'
                         <<session.endpoint.port<<": "<<error.what()<<"; retrying\n";
                std::this_thread::sleep_for(kReconnectDelay);
                continue;
            }
        }

        try
        {
            //测试用：中心断线需要手动重启中心，代理会一直尝试重连
            if(!send_file(session.fd.get(),files[file_index],pace_us,section_blocks))
            {
                session.stopped=true;
                return;
            }
            file_index++;
        }catch(const std::exception &error)
        {
            std::cerr<<"proxy transfer interrupted "<<session.endpoint.ip<<':'
                     <<session.endpoint.port<<": "<<error.what()<<"; reconnecting\n";
            session.fd.reset(-1);
            std::this_thread::sleep_for(kReconnectDelay);
        }
    }

    try
    {
        send_control_frame(session.fd.get(),srcast::encode_central_session_end());
    }catch(const std::exception &error)
    {
        std::cerr<<"proxy session end failed "<<session.endpoint.ip<<':'<<session.endpoint.port<<": "<<error.what()<<'\n';
    }
}
}

int main(int argc,char **argv)try
{
    if(argc==2&&(std::string(argv[1])=="--help"||std::string(argv[1])=="-h"))
    {
        usage(std::cout,argv[0]);
        return 0;
    }
    const auto options=parse_options(argc,argv);
    std::cout
        <<"========================================\n"
        <<"SiteRepairCast\n"
        <<"ROLE: CENTRAL\n"
        <<"========================================\n";
    for(std::size_t index=0;index<options.endpoints.size();index++)
    {
        std::cout<<"proxy["<<index<<"]="<<options.endpoints[index].ip<<':'
                 <<options.endpoints[index].port<<'\n';
    }
    std::cout<<"pace_us="<<options.pace_us<<'\n'
             <<"section_blocks="<<options.section_blocks<<'\n'
             <<"files="<<options.files.size()<<'\n';
    for(std::size_t index=0;index<options.files.size();index++)
    {
        std::cout<<"file["<<index<<"]="<<options.files[index]<<'\n';
    }
    std::cout<<std::flush;
    std::vector<ProxySession>sessions;
    for(const auto &endpoint:options.endpoints)
    {
        ProxySession session;
        session.endpoint=endpoint;
        sessions.push_back(std::move(session));
    }

    std::vector<std::thread>workers;
    workers.reserve(sessions.size());//注意现在n个代理n个线程
    for(auto &session:sessions)
    {
        workers.emplace_back([&session,&options]{run_proxy_session(session,options.files,
                                                 options.pace_us,options.section_blocks);});
    }
    for(auto &worker:workers)worker.join();
    for(const auto &session:sessions)
    {
        if(session.stopped)return 0;
    }
    std::cout<<"central session ended\n";
    return 0;
}catch(const std::exception &error)
{
    std::cerr<<"central sender error: "<<error.what()<<'\n';
    return 1;
}
