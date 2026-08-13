#pragma once

#include<cerrno>
#include<cstring>
#include<stdexcept>
#include<string>

[[noreturn]] inline void system_error(const std::string&operation)
{
    throw std::runtime_error(operation+": "+std::strerror(errno));
}