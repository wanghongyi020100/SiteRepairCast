#pragma once

#include"system_utils.hpp"

#include<sys/statvfs.h>
#include<cerrno>
#include<cstdint>
#include<cstdlib>
#include<filesystem>
#include<limits>
#include<string>

constexpr std::uint64_t kDiskSafetyMarginBytes=4*1024*1024;

inline void require_disk_space(const std::string &directory,std::uint64_t bytes_needed,
                               const std::string &label)
{
    struct statvfs info{};
    if(::statvfs(directory.c_str(),&info)!=0)system_error("statvfs "+label);
    const auto available=static_cast<std::uint64_t>(info.f_bavail)*
                         static_cast<std::uint64_t>(info.f_frsize);
    const auto required=bytes_needed+kDiskSafetyMarginBytes;
    if(available<required)
    {
        throw std::runtime_error(label+" has insufficient disk space");
    }
}

inline std::uint64_t storage_limit(const char *name)
{
    const char*raw=std::getenv(name);
    if(raw==nullptr||*raw=='\0')return 0;
    char *end=nullptr;
    errno=0;
    const auto value=std::strtoull(raw,&end,10);
    if(raw==end||*end!='\0'||errno==ERANGE)
    {
        throw std::runtime_error(std::string("invalid ")+name+" value");
    }
    return static_cast<std::uint64_t>(value);
}

inline std::uint64_t directory_usage(const std::string &directory)
{
    std::uint64_t total=0;
    std::error_code error;
    for(const auto &entry:std::filesystem::recursive_directory_iterator(directory,error))
    {
        if(error)throw std::runtime_error("scan storage directory failed: "+error.message());
        if(!entry.is_regular_file(error))
        {
            if(error)
            {
                throw std::runtime_error("stat storage file failed: "+error.message());
            }
            continue;
        }
        const auto size=entry.file_size(error);
        if(error||size>std::numeric_limits<std::uint64_t>::max()-total)
        {
            throw std::runtime_error("storage usage is too large");
        }
        total+=size;
    }
    if(error)
    {
        throw std::runtime_error("scan storage directory failed: "+error.message());
    }
    return total;
}

inline void require_storage_limit(const std::string &directory,std::uint64_t additional,
                                  const char *limit_name,const std::string &label)
{
    const auto limit=storage_limit(limit_name);
    if(limit==0)return;
    const auto used=directory_usage(directory);
    if(used>limit||additional>limit-used)
    {
        throw std::runtime_error(label+" exceeds "+limit_name);
    }
}