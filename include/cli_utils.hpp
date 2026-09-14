#pragma once

#include<arpa/inet.h>
#include<cerrno>
#include<cstdint>
#include<cstdlib>
#include<limits>
#include<stdexcept>
#include<string>

namespace srcast::cli
{
    inline unsigned long long parse_unsigned(const std::string &option,
                                              const std::string &value)
    {
        if(value.empty()||value.front()=='-')
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }

        char *end=nullptr;
        errno=0;
        const auto parsed=std::strtoull(value.c_str(),&end,10);
        if(value.c_str()==end||*end!='\0'||errno==ERANGE)
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }
        return parsed;
    }

    inline int parse_port(const std::string &option,const std::string &value)
    {
        const auto parsed=parse_unsigned(option,value);
        if(parsed==0||parsed>65535)
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }
        return static_cast<int>(parsed);
    }

    inline int parse_nonnegative_int(const std::string &option,const std::string &value)
    {
        const auto parsed=parse_unsigned(option,value);
        if(parsed>static_cast<unsigned long long>(std::numeric_limits<int>::max()))
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }
        return static_cast<int>(parsed);
    }

    inline int parse_positive_int(const std::string &option,const std::string &value)
    {
        const auto parsed=parse_nonnegative_int(option,value);
        if(parsed==0)
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }
        return parsed;
    }

    inline std::uint32_t parse_positive_u32(const std::string &option,
                                            const std::string &value)
    {
        const auto parsed=parse_unsigned(option,value);
        if(parsed==0||parsed>std::numeric_limits<std::uint32_t>::max())
        {
            throw std::runtime_error("invalid "+option+": "+value);
        }
        return static_cast<std::uint32_t>(parsed);
    }

    inline in_addr parse_ipv4(const std::string &option,const std::string &value)
    {
        in_addr address{};
        if(::inet_pton(AF_INET,value.c_str(),&address)!=1)
        {
            throw std::runtime_error("invalid "+option+": "+value+" is not IPv4");
        }
        return address;
    }

    inline in_addr parse_multicast_ipv4(const std::string &option,
                                        const std::string &value)
    {
        const auto address=parse_ipv4(option,value);
        if(!IN_MULTICAST(ntohl(address.s_addr)))
        {
            throw std::runtime_error("invalid "+option+": "+value+" is not multicast");
        }
        return address;
    }
}
