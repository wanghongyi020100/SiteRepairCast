#include"checksum.hpp"
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
    bool failed{false};
    bool stopped{false};
};

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

std::uint64_t create_transfer_id(const std::array<std::uint8_t,srcast::kSha256Size>&digest)
{
    std::uint64_t value=0;
    for(std::size_t index=0;index<sizeof(value);index++)value=(value<<8U)|digest[index];
    return value;
}

std::uint32_t stop_after_sections_for_test()
{
    const char*raw=std::getenv("SRCAST_STOP_AFTER_SECTIONS");
    if(raw==nullptr||*raw=='\0')return 0;

    char*end=nullptr;
    errno=0;
    const auto value=std::strtoul(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE||value>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error("invalid SRCAST_STOP_AFTER_SECTIONS value");
    }
    return static_cast<std::uint32_t>(value);
}

void usage(const char*program)
{
    std::cerr
<<"Usage: "<<program
<<"<proxy_ip><central_port><pace_us><file1>[file2 ...]\n"
<<"   or: "<<program
<<"--pace-us<pace_us>--proxy<proxy_ip><central_port>"
<<" [--proxy<proxy_ip><central_port>...]<file1>[file2 ...]\n"
<<"Example: "<<program
<<" 127.0.0.1 7000 0 input-a.bin input-b.bin\n";
}

struct CentralOptions
{
    std::vector<ProxyEndpoint>endpoints;
    std::vector<std::string>files;
    int pace_us{};
    std::uint32_t section_blocks{};
};

CentralOptions parse_options(int argc,char**argv)
{
    if(argc<5)
    {
        usage(argv[0]);
        throw std::runtime_error("not enough command-line arguments");
    }

    CentralOptions options;
    const bool multi_mode=std::string(argv[1])=="--proxy"||std::string(argv[1])=="--pace-us";
    if(multi_mode)
    {
        for(int index=1;index<argc;index++)
        {
            const std::string option=argv[index];
            if(option=="--pace-us")
            {
                if(++index>=argc)
                {
                    throw std::runtime_error("missing--pace-us value");
                }
                options.pace_us=std::stoi(argv[index]);
                continue;
            }
            if(option=="--proxy")
            {
                if(index+2>=argc)
                {
                    throw std::runtime_error("missing--proxy value");
                }
                options.endpoints.push_back({argv[++index],std::stoi(argv[++index])});
                continue;
            }
            options.files.emplace_back(option);
        }
    }
    else
    {
        options.endpoints.push_back({argv[1],std::stoi(argv[2])});
        options.pace_us=std::stoi(argv[3]);
        for(int index=4;index<argc;index++)options.files.emplace_back(argv[index]);
    }

    if(options.endpoints.empty()||options.files.empty()||options.pace_us<0)
    {
        throw std::runtime_error("invalid command-line argument");
    }
    for(const auto &endpoint:options.endpoints)
    {
        if(endpoint.port<1||endpoint.port>65535||endpoint.ip.empty())
        {
            throw std::runtime_error("invalid proxy endpoint");
        }
    }
    options.section_blocks=section_blocks_from_env();
    return options;
}

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
    std::cout<<"central sending file="<<file_path
<<" transfer_id="<<transfer_id
<<" size="<<file_size
<<" blocks="<<total_blocks<<'\n';
    send_control_frame(central_fd,srcast::encode_central_file_meta(meta));
    const auto section_count=srcast::section_count_for_blocks(total_blocks,section_blocks);

    const auto resume_frame=receive_control_frame(central_fd);
    const auto resume=srcast::decode_central_resume(resume_frame);
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

    std::cout<<"central resume transfer_id="<<transfer_id<<" next_section="<<resume.next_section_id
<<'/'<<section_count<<'\n';
    std::vector<std::uint8_t>payload;
    const auto stop_after_sections=stop_after_sections_for_test();
    std::uint32_t confirmed_this_run=0;

    for(std::uint32_t section_id=resume.next_section_id;section_id<section_count;section_id++)
    {
        const auto first_block=srcast::section_first_block(section_id,section_blocks);
        const auto section_block_count=srcast::section_block_count(total_blocks,section_id,
                                               meta.section_block_count);
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

        send_control_frame(central_fd,srcast::encode_central_file_end
                          ({transfer_id,section_id,total_blocks}));

        const auto response=receive_control_frame(central_fd);
        const auto result=srcast::decode_central_status(response);
        if(result.transfer_id!=transfer_id||result.status!=srcast::CentralStatusCode::Cached||
           result.file_size!=file_size)
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

            std::cout<<"test hook closing central connection after "
<<confirmed_this_run<<" confirmed sections\n";
            return false;
        }
    }

    std::cout<<"central confirmed cached transfer_id="<<transfer_id<<'\n';
    return true;
}

void run_proxy_session(ProxySession&session,const std::vector<std::string>&files,
                       int pace_us,std::uint32_t section_blocks)
{
    try
    {
        for(const auto &file:files)
        {
            if(!send_file(session.fd.get(),file,pace_us,section_blocks))
            {
                session.stopped=true;
                return;
            }
        }
        send_control_frame(session.fd.get(),srcast::encode_central_session_end());
    }catch(const std::exception&error)
    {
        session.failed=true;
        std::cerr<<"proxy transfer failed "<<session.endpoint.ip<<':'
<<session.endpoint.port<<": "<<error.what()<<'\n';
        session.fd.reset(-1);
    }
}
}

int main(int argc,char**argv)try
{
    const auto options=parse_options(argc,argv);
    std::vector<ProxySession>sessions;
    bool connection_failed=false;
    for(const auto &endpoint:options.endpoints)
    {
        try
        {
            sessions.push_back({endpoint,connect_proxy(endpoint.ip,endpoint.port)});
        }catch(const std::exception&error)
        {
            connection_failed=true;
            std::cerr<<"proxy connection failed "<<endpoint.ip
<<':'<<endpoint.port<<": "<<error.what()<<'\n';
        }
    }
    if(sessions.empty())
    {
        throw std::runtime_error("no proxy connection available");
    }

    std::vector<std::thread>workers;
    workers.reserve(sessions.size());
    for(auto &session:sessions)
    {
        workers.emplace_back([&session,&options]{run_proxy_session(session,options.files,
                                                 options.pace_us,options.section_blocks);});
    }
    for(auto &worker:workers)worker.join();
    for(const auto &session:sessions)
    {
        connection_failed=connection_failed||session.failed;
        if(session.stopped)return 0;
    }
    std::cout<<"central session ended\n";
    return connection_failed?1:0;
}catch(const std::exception &error)
{
    std::cerr<<"central sender error: "<<error.what()<<'\n';
    return 1;
}