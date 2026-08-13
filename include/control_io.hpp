#pragma once

#include"protocol.hpp"
#include"system_utils.hpp"

#include<arpa/inet.h>
#include<sys/socket.h>
#include<cerrno>
#include<cstdint>
#include<stdexcept>
#include<vector>

inline void write_all(int fd,const std::uint8_t*data,std::size_t size)
{
    std::size_t written=0;
    while(written<size)
    {
        const auto count=::send(fd,data+written,size-written,MSG_NOSIGNAL);
        if(count<0)
        {
            if(errno==EINTR)continue;
            system_error("send control frame");
        }
        if(count==0)
        {
            throw std::runtime_error("control connection made no write progress");
        }
        written+=static_cast<std::size_t>(count);
    }
}

inline void read_all(int fd,std::uint8_t*data,std::size_t size)
{
    std::size_t received=0;
    while(received<size)
    {
        const auto count=::recv(fd,data+received,size-received,0);
        if(count<0)
        {
            if(errno==EINTR)continue;
            system_error("recv control frame");
        }
        if(count==0)
        {
            throw std::runtime_error("control connection closed");
        }
        received+=static_cast<std::size_t>(count);
    }
}

inline void send_control_frame(int fd,const std::vector<std::uint8_t>&frame)
{
    if(frame.empty()||frame.size()>srcast::kMaxControlFrameSize)
    {
        throw std::runtime_error("invalid control frame size");
    }
    const auto network_size=htonl(static_cast<std::uint32_t>(frame.size()));
    write_all(fd,reinterpret_cast<const std::uint8_t*>
             (&network_size),sizeof(network_size));
    write_all(fd,frame.data(),frame.size());
}

inline std::vector<std::uint8_t>receive_control_frame(int fd)
{
    std::uint32_t network_size{};
    read_all(fd,reinterpret_cast<std::uint8_t*>(&network_size),sizeof(network_size));
    const auto size=ntohl(network_size);
    if(size==0||size>srcast::kMaxControlFrameSize)
    {
        throw std::runtime_error("invalid received control frame size");
    }
    std::vector<std::uint8_t>frame(size);
    read_all(fd,frame.data(),frame.size());
    return frame;
}
