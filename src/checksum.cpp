#include"checksum.hpp"

#include<openssl/evp.h>
#include<array>
#include<cerrno>
#include<cstring>
#include<fstream>
#include<iomanip>
#include<memory>
#include<sstream>
#include<stdexcept>
namespace srcast
{
std::uint32_t crc32(const std::uint8_t*data,std::size_t size)
{
    static const std::array<std::uint32_t,256>table=[] {
        std::array<std::uint32_t,256>result{};
        for(std::uint32_t i=0;i<result.size();i++)
        {
            std::uint32_t value=i;
            for(int bit=0;bit<8;bit++)
            {
                value=(value&1U)?(0xedb88320U^(value>>1U)):(value>>1U);
            }
            result[i]=value;
        }
        return result;
    }();
    std::uint32_t value=0xffffffffU;
    for(std::size_t i=0;i<size;i++)
    {
        value=table[(value^data[i])&0xffU]^(value>>8U);
    }
    return value^0xffffffffU;
}

std::array<std::uint8_t,32>sha256_file(const std::string&path)
{
    std::ifstream input(path,std::ios::binary);
    if(!input)
    {
        throw std::runtime_error("cannot open file for SHA-256: "+path);
    }

    using ContextPtr=std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)>;
    ContextPtr context(EVP_MD_CTX_new(),&EVP_MD_CTX_free);
    if(!context||EVP_DigestInit_ex(context.get(),EVP_sha256(),nullptr)!=1)
    {
        throw std::runtime_error("EVP_DigestInit_ex failed");
    }

    std::array<char,64*1024>buffer{};
    while(input)
    {
        input.read(buffer.data(),static_cast<std::streamsize>(buffer.size()));
        const auto count=input.gcount();
        if(count>0&&EVP_DigestUpdate(context.get(),buffer.data(),static_cast<std::size_t>(count))!=1)
        {
            throw std::runtime_error("EVP_DigestUpdate failed");
        }
    }
    if(!input.eof())
    {
        throw std::runtime_error("failed while reading file for SHA-256: "+path);
    }

    std::array<std::uint8_t,32>digest{};
    unsigned int digest_size=0;
    if(EVP_DigestFinal_ex(context.get(),digest.data(),&digest_size)!=1||digest_size!=digest.size())
    {
        throw std::runtime_error("EVP_DigestFinal_ex failed");
    }
    return digest;
}

std::string hex_digest(const std::array<std::uint8_t,32>&digest)
{
    std::ostringstream output;
    output<<std::hex<<std::setfill('0');
    for(const auto byte:digest)
    {output<<std::setw(2)<<static_cast<unsigned int>(byte);}
    return output.str();
}

}
