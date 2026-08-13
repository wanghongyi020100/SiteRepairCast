#pragma once

#include"protocol.hpp"
#include"system_utils.hpp"

#include<algorithm>
#include<array>
#include<cerrno>
#include<cstdint>
#include<string>
#include<vector>
#include<sys/types.h>
#include<unistd.h>

//文件按block读取，中心发送端代理补传用同一套边界处理
inline void read_block(int input_fd,const std::string &file_path,std::uint64_t file_size,
                       std::uint32_t block_id,std::vector<std::uint8_t>&payload,std::uint64_t &offset)
{
    offset=static_cast<std::uint64_t>(block_id)*srcast::kPayloadSize;
    const auto wanted=static_cast<std::size_t>(
        std::min<std::uint64_t>(srcast::kPayloadSize,file_size-offset));
    payload.assign(wanted,0);
    std::size_t received=0;
    while(received<wanted)
    {
        const auto count=::pread(input_fd,payload.data()+received,wanted-received,
                                 static_cast<off_t>(offset+received));
        if(count<0)
        {
            if(errno==EINTR)continue;
            system_error("pread "+file_path);
        }
        if(count==0)
        {
            throw std::runtime_error("unexpected EOF while reading source file: "+file_path);
        }
        received+=static_cast<std::size_t>(count);
    }
}

inline void read_block(int input_fd,const std::string&file_path,std::uint64_t file_size,
                       std::uint32_t block_id,
                       std::array<std::uint8_t,srcast::kPayloadSize>&buffer,
                       std::uint64_t &offset,std::uint16_t &payload_size)
{
    offset=static_cast<std::uint64_t>(block_id)*srcast::kPayloadSize;
    const auto wanted=static_cast<std::size_t>(
        std::min<std::uint64_t>(srcast::kPayloadSize,file_size-offset));
    std::size_t received=0;
    while(received<wanted)
    {
        const auto count=::pread(input_fd,buffer.data()+received,wanted-received,
                                 static_cast<off_t>(offset+received));
        if(count<0)
        {
            if(errno==EINTR)continue;
            system_error("pread "+file_path);
        }
        if(count==0)
        {
            throw std::runtime_error("unexpected EOF while reading source file: "+file_path);
        }
        received+=static_cast<std::size_t>(count);
    }
    payload_size=static_cast<std::uint16_t>(wanted);
}

inline void write_all_at(int fd,const std::uint8_t *data,std::size_t size,std::uint64_t offset)
{
    std::size_t written=0;
    while(written<size)
    {
        const auto count=::pwrite(fd,data+written,size-written,
                                  static_cast<off_t>(offset+written));
        if(count<0)
        {
            if(errno==EINTR)continue;
            system_error("pwrite");
        }
        if(count==0)
        {
            throw std::runtime_error("pwrite made no progress");
        }
        written+=static_cast<std::size_t>(count);
    }
}