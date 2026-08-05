#include"checksum.hpp"
#include"protocol.hpp"

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
            system_error("send central frame");
        }
        if(count==0)
        {
            throw std::runtime_error("central connection made no write progress");
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
            system_error("recv central frame");
        }
        if(count==0)
        {
            throw std::runtime_error("central connection closed");
        }
        received+=static_cast<std::size_t>(count);
    }
}

void send_control_frame(int fd,const std::vector<std::uint8_t>&frame)
{
    if(frame.empty()||frame.size()>srcast::kMaxControlFrameSize)
    {
        throw std::runtime_error("invalid central frame size");
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
        throw std::runtime_error("invalid central response frame size");
    }

    std::vector<std::uint8_t>frame(size);
    read_all(fd,frame.data(),frame.size());
    return frame;
}

FileDescriptor connect_proxy(
    const std::string&proxy_ip,
    int central_port)
    {

    FileDescriptor fd(::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0));
    if(fd.get()<0)
    {
        system_error("socket central");
    }

    sockaddr_in proxy{};
    proxy.sin_family=AF_INET;
    proxy.sin_port=htons(static_cast<std::uint16_t>(central_port));
    if(::inet_pton(AF_INET,proxy_ip.c_str(),&proxy.sin_addr)!=1)
    {
        throw std::runtime_error("invalid proxy IPv4 address");
    }

    for(;;)
    {
        if(::connect(
                fd.get(),
                reinterpret_cast<sockaddr*>(&proxy),
                sizeof(proxy))==0)
                {
            return fd;
        }
        if(errno==EINTR)
        {
            continue;
        }
        system_error("connect proxy central port");
    }
}

std::uint64_t create_transfer_id(
    const std::array<std::uint8_t,srcast::kSha256Size>&digest)
    {

    std::uint64_t value=0;
    for(std::size_t index=0; index<sizeof(value);++index)
    {
        value=(value<<8U)|digest[index];
    }
    return value;
}

std::uint32_t stop_after_sections_for_test()
{
    const char*raw=std::getenv("SRCAST_STOP_AFTER_SECTIONS");
    if(raw==nullptr||*raw=='\0')
    {
        return 0;
    }

    char*end=nullptr;
    errno=0;
    const auto value=std::strtoul(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE||
        value>std::numeric_limits<std::uint32_t>::max())
        {
        throw std::runtime_error(
            "invalid SRCAST_STOP_AFTER_SECTIONS value");
    }
    return static_cast<std::uint32_t>(value);
}

void read_block(
    int input_fd,
    const std::string&file_path,
    std::uint64_t file_size,
    std::uint32_t block_id,
    std::vector<std::uint8_t>&payload,
    std::uint64_t&offset)
    {

    offset=static_cast<std::uint64_t>(block_id) * srcast::kPayloadSize;
    const auto wanted=static_cast<std::size_t>(
        std::min<std::uint64_t>(srcast::kPayloadSize,file_size-offset));

    payload.assign(wanted,0);
    std::size_t received=0;
    while(received<wanted)
    {
        const auto count=::pread(
            input_fd,
            payload.data()+received,
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
}

void usage(const char*program)
{
    std::cerr
<<"Usage: "<<program
<<"<proxy_ip><central_port><pace_us><file1>[file2 ...]\n"
<<"Example: "<<program
<<" 127.0.0.1 7000 0 input-a.bin input-b.bin\n";
}

bool send_file(
    int central_fd,
    const std::string&file_path,
    int pace_us)
    {

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
    meta.sha256=digest;

    std::cout<<"central sending file="<<file_path
<<" transfer_id="<<transfer_id
<<" size="<<file_size
<<" blocks="<<total_blocks<<'\n';

    send_control_frame(
        central_fd,
        srcast::encode_central_file_meta(meta));

    const auto section_count=srcast::section_count_for_blocks(total_blocks);
    const auto resume_frame=receive_control_frame(central_fd);
    const auto resume=srcast::decode_central_resume(resume_frame);
    if(resume.transfer_id!=transfer_id||
        resume.file_size!=file_size||
        resume.total_sections!=section_count||
        resume.next_section_id>section_count)
        {
        throw std::runtime_error(
            "invalid CENTRAL_RESUME from proxy: "+file_path);
    }
    if(resume.next_section_id==section_count)
    {
        if(resume.sha256!=digest)
        {
            throw std::runtime_error(
                "proxy cached digest mismatch: "+file_path);
        }
        std::cout<<"central transfer already cached transfer_id="
<<transfer_id<<'\n';
        return true;
    }

    std::cout<<"central resume transfer_id="<<transfer_id
<<" next_section="<<resume.next_section_id
<<'/'<<section_count<<'\n';

    std::vector<std::uint8_t>payload;
    const auto stop_after_sections=stop_after_sections_for_test();
    std::uint32_t confirmed_this_run=0;
    for(std::uint32_t section_id=resume.next_section_id;
         section_id<section_count;
++section_id)
         {
        const auto first_block=srcast::section_first_block(section_id);
        const auto section_blocks=srcast::section_block_count(
            total_blocks,
            section_id);

        for(std::uint32_t local_block=0;
             local_block<section_blocks;
++local_block)
             {
            const auto block_id=first_block+local_block;
            std::uint64_t offset{};
            read_block(
                input.get(),
                file_path,
                file_size,
                block_id,
                payload,
                offset);

            srcast::CentralDataMessage data;
            data.transfer_id=transfer_id;
            data.section_id=section_id;
            data.block_id=block_id;
            data.offset=offset;
            data.payload_size=static_cast<std::uint16_t>(payload.size());
            data.crc32=srcast::crc32(payload.data(),payload.size());
            data.payload=payload;

            send_control_frame(
                central_fd,
                srcast::encode_central_data(data));

            if(pace_us>0)
            {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(pace_us));
            }
        }

        send_control_frame(
            central_fd,
            srcast::encode_central_file_end(
                {transfer_id,section_id,total_blocks}));

        const auto response=receive_control_frame(central_fd);
        const auto result=srcast::decode_central_status(response);
        if(result.transfer_id!=transfer_id||
            result.status!=srcast::CentralStatusCode::Cached||
            result.file_size!=file_size)
            {
            throw std::runtime_error(
                "proxy did not confirm cached section: "+file_path);
        }
        if(section_id+1U==section_count && result.sha256!=digest)
        {
            throw std::runtime_error(
                "proxy final digest mismatch: "+file_path);
        }
        std::cout<<"central confirmed section="<<section_id
<<" transfer_id="<<transfer_id<<'\n';

++confirmed_this_run;
        if(stop_after_sections!=0 &&
            confirmed_this_run>=stop_after_sections &&
            section_id+1U<section_count)
            {
            std::cout<<"test hook closing central connection after "
<<confirmed_this_run<<" confirmed sections\n";
            return false;
        }
    }

    std::cout<<"central confirmed cached transfer_id="
<<transfer_id<<'\n';
    return true;
}

}

int main(int argc,char** argv) try {
    if(argc<5)
    {
        usage(argv[0]);
        return 2;
    }

    const std::string proxy_ip=argv[1];
    const int central_port=std::stoi(argv[2]);
    const int pace_us=std::stoi(argv[3]);

    if(central_port<1||central_port>65535||pace_us<0)
    {
        throw std::runtime_error("invalid command-line argument");
    }

    auto central_fd=connect_proxy(proxy_ip,central_port);
    for(int index=4; index<argc;++index)
    {
        if(!send_file(central_fd.get(),argv[index],pace_us))
        {
            return 0;
        }
    }

    send_control_frame(
        central_fd.get(),
        srcast::encode_central_session_end());

    std::cout<<"central session ended\n";
    return 0;

} catch(const std::exception&error) {
    std::cerr<<"central sender error: "<<error.what()<<'\n';
    return 1;
}
