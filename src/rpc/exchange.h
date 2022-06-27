#pragma once

#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <lowfive/metadata/serialization.hpp>

namespace LowFive
{

namespace rpc
{

struct stream: public BinaryBuffer
{
    virtual void flush()                        {}
};

struct in_stream: public stream
{
            in_stream(int fno)                  { fd_ = fdopen(fno, "rb"); if (!fd_) throw std::runtime_error("Unable to open file descriptor"); }
            in_stream(std::string fn)           { fd_ = fopen(fn.c_str(), "rb"); if (!fd_) throw std::runtime_error("Unable to open: " + fn); }
            ~in_stream()                        { fclose(fd_); }

    void    save_binary(const char* x, size_t count) override   { throw std::invalid_argument("Cannot write to an in_stream"); }
    void    load_binary(char* x, size_t count) override         { fread(x, count, 1, fd_); }

    FILE* fd_;
};

struct out_stream: public stream
{
            out_stream(int fno)                 { fd_ = fdopen(fno, "wb"); if (!fd_) throw std::runtime_error("Unable to open file descriptor"); }
            out_stream(std::string fn)          { fd_ = fopen(fn.c_str(), "wb"); if (!fd_) throw std::runtime_error("Unable to open: " + fn); }
            ~out_stream()                       { fclose(fd_); }

    void    save_binary(const char* x, size_t count) override   { fwrite(x, count, 1, fd_); }
    void    load_binary(char* x, size_t count) override         { throw std::invalid_argument("Cannot read from an out_stream"); }

    void    flush() override                                    { fflush(fd_); }

    FILE* fd_;
};

struct exchange
{
        exchange(stream& in, stream& out):
            in_(in), out_(out)              {}

    template<class T>
    void        send(const T& x)            { save(out_, x); }

    template<class T>
    void        recv(T& x)                  { load(in_, x); }

    void        flush()                     { out_.flush(); }

    stream&     in_;
    stream&     out_;
};


void send(rpc::stream& out, std::string s)
{
    out.save_binary(s.c_str(), s.size());
    out.flush();
}

namespace detail
{

bool subset_match(const std::vector<char>& buffer, size_t from, size_t to, const std::string& s, size_t position)
{
    for (size_t i = from; i < to; ++i)
        if (s[position + i] != buffer[i])
            return false;

    return true;
}

}

void wait(rpc::stream& in, std::string s)
{
    // read from in_stream until the expected string is found
    // algorithm: read string size characters; if prefix match, read more (of what's missing);
    //            if not read more characters

    std::vector<char> buffer(s.size());
    size_t position = 0;
    while(position != s.size())
    {
        size_t missing = s.size() - position;
        in.load_binary(&buffer[0], missing);

        bool found = false;
        for (size_t i = 0; i < missing; ++i)
        {
            if (detail::subset_match(buffer, i, missing, s, position))
            {
                found = true;
                position += (missing - i);
                break;
            }
        }
        if (!found)     // reset
            position = 0;
    }
}

}   // namespace rpc
}   // namespace LowFive
