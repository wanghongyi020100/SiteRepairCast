#pragma once

#include"protocol.hpp"

#include<cerrno>
#include<cstdint>
#include<cstdlib>
#include<limits>
#include<stdexcept>

inline std::uint32_t section_blocks_from_env()
{
    const char*raw=std::getenv("SRCAST_SECTION_BLOCKS");
    if(raw==nullptr||*raw=='\0')return srcast::kDefaultSectionBlockCount;
    char*end=nullptr;
    errno=0;
    const auto value=std::strtoul(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE||value==0||
       value>std::numeric_limits<std::uint32_t>::max())
    {
        throw std::runtime_error("invalid SRCAST_SECTION_BLOCKS value");
    }
    return static_cast<std::uint32_t>(value);
}