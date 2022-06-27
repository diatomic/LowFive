#pragma once

#include <vector>

#include <sys/ioctl.h>

#include "mpi.hpp"
#include "exchange.h"

namespace LowFive
{

namespace rpc
{

// repeater streams
struct mpi_in_stream: public stream
{
            mpi_in_stream(mpi::communicator world, std::string fn):
                world_(world)                                   { if (world_.rank() != 0) return; fd_ = fopen(fn.c_str(), "rb"); if (!fd_) throw std::runtime_error("Unable to open: " + fn); fd_no_ = fileno(fd_); }
            mpi_in_stream(mpi::communicator world, int fno):
                world_(world)                                   { if (world_.rank() != 0) return; fd_ = fdopen(fno, "rb"); if (!fd_) throw std::runtime_error("Unable to open file descriptor"); fd_no_ = fno; }
            ~mpi_in_stream()                                    { if (world_.rank() == 0) fclose(fd_); }

    void    save_binary(const char* x, size_t count) override   { throw std::invalid_argument("Cannot write to an in_stream"); }
    void    load_binary(char* x, size_t count) override
    {
        while (count > 0)
        {
            size_t to_read = std::min(count, buffer_.size() - position_);
            if (to_read == 0)       // position_ == buffer_.size()
            {
                // read and broadcast
                position_ = 0;
                if (world_.rank() == 0)
                {
                    int num_bytes;
                    int err = ioctl(fd_no_, FIONREAD, &num_bytes);
                    if (err != 0)
                        throw std::runtime_error("Error in ioctl");
                    buffer_.resize(num_bytes);
                    fread(&buffer_[0], sizeof(char), buffer_.size(), fd_);
                    mpi::broadcast(world_, buffer_, 0);
                } else
                    mpi::broadcast(world_, buffer_, 0);
            } else
            {
                std::copy(&buffer_[position_], &buffer_[position_ + to_read], x);
                position_ += to_read;
                x += to_read;
                count -= to_read;
            }
        }
    }

    mpi::communicator   world_;
    FILE*               fd_;
    int                 fd_no_;

    std::vector<char>   buffer_;
    size_t              position_ = 0;
};

// stream that only writes out data on rank == 0
struct mpi_out_stream: public stream
{
            mpi_out_stream(mpi::communicator world, std::string fn):
                world_(world)                                   { if (world_.rank() != 0) return; fd_ = fopen(fn.c_str(), "wb"); if (!fd_) throw std::runtime_error("Unable to open: " + fn); }
            mpi_out_stream(mpi::communicator world, int fno):
                world_(world)                                   { if (world_.rank() != 0) return; fd_ = fdopen(fno, "wb"); if (!fd_) throw std::runtime_error("Unable to open file descriptor"); }
            ~mpi_out_stream()                                   { if (world_.rank() == 0) fclose(fd_); }

    void    save_binary(const char* x, size_t count) override   { if (world_.rank() == 0) fwrite(x, count, 1, fd_); }
    void    load_binary(char* x, size_t count) override         { throw std::invalid_argument("Cannot read from an out_stream"); }

    void    flush() override                                    { if (world_.rank() == 0) fflush(fd_); }

    mpi::communicator   world_;
    FILE*               fd_;
};

}   // namespace rpc
}   // namespace LowFive
