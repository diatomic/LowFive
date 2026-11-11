#pragma once

#include <mpi.h>
#include <utility>
#include <diy/mpi.hpp>
#include "../metadata/serialization.hpp"

#include "../log-private.hpp"

namespace LowFive
{

namespace rpc
{

inline void send(diy::mpi::communicator comm, int dest, int tag, diy::MemoryBuffer& bb)
{
    auto log = get_logger();

    comm.send(dest, tag, bb.buffer);
    size_t nblobs = bb.nblobs();
    comm.send(dest, tag, nblobs);
    for (size_t i = 0; i < nblobs; ++i)
    {
        auto blob = bb.load_binary_blob();

        diy::detail::VectorWindow<char> window;
        window.begin = const_cast<char*>(blob.pointer.get());
        window.count = blob.size;
        comm.send(dest, tag, window);
    }
}

inline void ssend(diy::mpi::communicator comm, int dest, int tag, diy::MemoryBuffer& bb)
{
    auto log = get_logger();

    comm.ssend(dest, tag, bb.buffer);
    size_t nblobs = bb.nblobs();
    comm.ssend(dest, tag, nblobs);
    for (size_t i = 0; i < nblobs; ++i)
    {
        auto blob = bb.load_binary_blob();

        diy::detail::VectorWindow<char> window;
        window.begin = const_cast<char*>(blob.pointer.get());
        window.count = blob.size;
        comm.ssend(dest, tag, window);
    }
}

inline void recv(diy::mpi::communicator comm, int source, int tag, diy::MemoryBuffer& bb)
{
    auto log = get_logger();

    comm.recv(source, tag, bb.buffer);
    size_t nblobs;
    comm.recv(source, tag, nblobs);
    for (size_t i = 0; i < nblobs; ++i)
    {
        auto status = comm.probe(source, tag);
        size_t count = status.count<char>();

        diy::detail::VectorWindow<char> window;

        diy::MemoryBuffer::Blob blob {
            { nullptr, [](const char* data) { delete[] data; } }, count
        };
        blob.pointer.reset(new char[count]);

        window.begin = const_cast<char*>(blob.pointer.get());
        window.count = count;

        comm.recv(source, tag, window);

        // Move the owner directly so failed insertion cannot leave two owners.
        bb.blobs.push_back(std::move(blob));
    }
}

}

}
