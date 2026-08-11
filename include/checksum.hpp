#pragma once

#include<array>
#include<cstddef>
#include<cstdint>
#include<string>
namespace srcast
{
    std::uint32_t crc32(const std::uint8_t*data,std::size_t size);
    std::array<std::uint8_t,32>sha256_file(const std::string&path);
    std::string hex_digest(const std::array<std::uint8_t,32>&digest);
}