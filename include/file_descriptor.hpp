#pragma once

#include<unistd.h>

class FileDescriptor
{
public:
    explicit FileDescriptor(int fd=-1):fd_(fd){}
    ~FileDescriptor(){if(fd_>=0)::close(fd_);}

    FileDescriptor(const FileDescriptor&)=delete;
    FileDescriptor&operator=(const FileDescriptor&)=delete;
    FileDescriptor(FileDescriptor&&other)noexcept:fd_(other.fd_){other.fd_=-1;}

    FileDescriptor&operator=(FileDescriptor&&other)noexcept
    {
        if(this!=&other)
        {
            reset(other.fd_);
            other.fd_=-1;
        }
        return *this;
    }

    int get()const{return fd_;}

    void reset(int fd)
    {
        if(fd_>=0)::close(fd_);
        fd_=fd;
    }

private:
    int fd_;
};