#pragma once

#include <vector>

#include <diy/decomposition.hpp>
#include <diy/types.hpp>

#include "metadata.hpp"
#include "metadata/serialization.hpp"

namespace LowFive
{

// Common setup between Index and Query
struct IndexQuery
{
    using Bounds                = diy::Bounds<int64_t>;
    using Point                 = Bounds::Point;
    using Coordinate            = Bounds::Coordinate;
    using Decomposer            = diy::RegularDecomposer<Bounds>;
    using DivisionsVector       = Decomposer::DivisionsVector;
    using BlockID               = diy::BlockID;
    using Link                  = diy::RegularGridLink;

    using BoxLocations          = std::vector<std::tuple<Dataspace, int>>;

    using communicator          = diy::mpi::communicator;
    using communicators         = std::vector<communicator>;

    enum msgs   { dimension = 1, domain, redirect, data, done, ready, id, file, fnames };        // message type

    // tags indicate the source of communication, so that in the threaded
    // regime, they can be used to correctly distinguish between senders and
    // receipients
    enum tags   {
                    producer = 1,       // communication from producer
                    consumer            // communication from consumer
                };

                        IndexQuery(MPI_Comm local_, std::vector<MPI_Comm> intercomms_):
                            local(local_)
    {
        for (auto comm : intercomms_)
            intercomms.emplace_back(comm);
    }

    // serialize and send
    template<class T>
    static void         send(communicator& comm, int dest, int tag, int msg, const T& x)
    {
        diy::MemoryBuffer b;
        diy::save(b, x);
        send(comm, dest, tag, msg, b);
    }

    // serialize and send
    template<class T>
    static void         send(communicator& comm, int dest, int tag, int msg, int id, const T& x)
    {
        diy::MemoryBuffer b;
        diy::save(b, id);
        diy::save(b, x);
        send(comm, dest, tag, msg, b);
    }

    // send, using existing buffer
    static void      send(communicator& comm, int dest, int tag, int msg, diy::MemoryBuffer& b)
    {
        diy::save(b, msg);
        comm.send(dest, tag, b.buffer);
    }

    // recv and deserialize
    template<class T>
    static int      recv(communicator& comm, int source, int tag, T& x)
    {
        diy::MemoryBuffer b;
        comm.recv(source, tag, b.buffer);
        int msg;
        diy::load_back(b, msg);
        diy::load(b, x);
        return msg;
    }

    // recv message type, but keep the buffer
    static int      recv(communicator& comm, int source, int tag, diy::MemoryBuffer& b)
    {
        comm.recv(source, tag, b.buffer);
        int msg;
        diy::load_back(b, msg);
        return msg;
    }

    // serialize and broadcast (+ deserialize)
    template<class T>
    static void         broadcast(communicator& comm, T& x, int root)
    {
        diy::MemoryBuffer b;
        if (comm.rank() == root)
        {
            diy::save(b, x);
            diy::mpi::broadcast(comm, b.buffer, root);
        } else
        {
            diy::mpi::broadcast(comm, b.buffer, root);
            diy::load(b, x);
        }
    }

    static void         expected(int received_, int expected_)
    {
        if (received_ != expected_)
            throw std::runtime_error(fmt::format("Message mismatch: expected = {}, received {}", expected_, received_));
    }

    communicator&       intercomm(int i)         { return intercomms[i]; }

    communicator        local;
    communicators       intercomms;

    // TODO: move this into RegularDecomposer itself (this is a slightly modified point_to_gids)
    static
    std::vector<int>    bounds_to_gids(const Bounds& bounds, const Decomposer& decomposer)
    {
        int dim = decomposer.dim;
        std::vector< std::pair<Coordinate, Coordinate> > ranges(dim);
        for (int i = 0; i < dim; ++i)
        {
            Coordinate& bottom = ranges[i].first;
            Coordinate& top    = ranges[i].second;

            Coordinate l = bounds.min[i];
            Coordinate r = bounds.max[i];

            // taken from top_bottom
            top     = diy::detail::BoundsHelper<Bounds>::upper(r, decomposer.divisions[i], decomposer.domain.min[i], decomposer.domain.max[i], false /* share_face[axis] */);
            bottom  = diy::detail::BoundsHelper<Bounds>::lower(l, decomposer.divisions[i], decomposer.domain.min[i], decomposer.domain.max[i], false /* share_face[axis] */);

            if (true /* !wrap[i] */)
            {
                bottom  = std::max(static_cast<Coordinate>(0), bottom);
                top     = std::min(static_cast<Coordinate>(decomposer.divisions[i]), top);
            }
        }

        // look up gids for all combinations
        std::vector<int> gids;
        DivisionsVector coords(dim), location(dim);
        while(location.back() < ranges.back().second - ranges.back().first)
        {
            for (int i = 0; i < dim; ++i)
                coords[i] = ranges[i].first + location[i];
            gids.push_back(decomposer.coords_to_gid(coords, decomposer.divisions));

            location[0]++;
            int i = 0;
            while (i < dim-1 && location[i] == ranges[i].second - ranges[i].first)
            {
                location[i] = 0;
                ++i;
                location[i]++;
            }
        }

        return gids;
    }
};

}
