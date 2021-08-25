#pragma once

#include "index-query.hpp"

namespace LowFive
{

struct Query: public IndexQuery
{
    int                         id;
    int                         dim;
    Datatype                    type;
    Dataspace                   space;
    Decomposer                  decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy, overwritten in the constructor
    int                         intercomm_index;                                    // index of intercomm to use

    // consumer versions of the constructor

    Query(communicator& local_, communicators& intercomms_, int remote_size, std::string name, int intercomm_index_ = 0):
                              IndexQuery(local_, intercomms_), intercomm_index(intercomm_index_)
    {
        bool root = local.rank() == 0;

        // TODO: the broadcasts necessitate collective open; they are not
        //       necessary (or could be triggered by a hint from the execution framework)

        // query producer for dim
        if (root)
        {
            int msg;

            // query id using name
            send(intercomm(intercomm_index), 0, tags::consumer, msgs::id, name);
            msg = recv(intercomm(intercomm_index), 0, tags::producer, id); expected(msg, msgs::id);

            send(intercomm(intercomm_index), 0, tags::consumer, msgs::dimension, id, 0);
            msg = recv(intercomm(intercomm_index), 0, tags::producer, dim);   expected(msg, msgs::dimension);
            msg = recv(intercomm(intercomm_index), 0, tags::producer, type);  expected(msg, msgs::dimension);
            msg = recv(intercomm(intercomm_index), 0, tags::producer, space); expected(msg, msgs::dimension);
        }
        diy::mpi::broadcast(local, id,  0);
        diy::mpi::broadcast(local, dim, 0);
        broadcast(local, type, 0);
        broadcast(local, space, 0);

        // query producer for domain
        Bounds domain { dim };
        if (root)
        {
            send(intercomm(intercomm_index), 0, tags::consumer, msgs::domain, id, 0);
            int msg = recv(intercomm(intercomm_index), 0, tags::producer, domain);
            expected(msg, msgs::domain);
        }
        broadcast(local, domain, 0);

        decomposer = Decomposer(dim, domain, remote_size);
    }

    void                close()
    {
        local.barrier();

        if (local.rank() == 0)
            send(intercomm(intercomm_index), 0, tags::consumer, msgs::done, id, 0);
    }

    static
    void                close(communicator& local, communicator& intercomm)
    {
        local.barrier();

        if (local.rank() == 0)
            send(intercomm, 0, tags::consumer, msgs::done, 0 /* id */, 0);          // passing 0 for the id seems to work TODO: is this right?
    }

    void                query(const Dataspace&                     file_space,      // input: query in terms of file space
                              const Dataspace&                     mem_space,       // ouput: memory space of resulting data
                              void*                                buf)             // output: resulting data, allocated by caller
    {
        // enqueue queried file dataspace to the ranks that are
        // responsible for the boxes that (might) intersect them
        Bounds b { dim };           // diy representation of the dataspace's bounding box
        b.min = Point(file_space.min);
        b.max = Point(file_space.max);

        BoxLocations all_redirects;
        auto gids = bounds_to_gids(b, decomposer);
        for (int gid : gids)
        {
            // TODO: make this asynchronous (isend + irecv, etc)
            send(intercomm(intercomm_index), gid, tags::consumer, msgs::redirect, id, file_space);

            BoxLocations redirects;
            int msg = recv(intercomm(intercomm_index), gid, tags::producer, redirects); expected(msg, msgs::redirect);
            for (auto& x : redirects)
                all_redirects.push_back(x);
        }

        // request and receive data
        std::set<int> blocks;
        for (auto& y : all_redirects)
        {
            // TODO: make this asynchronous (isend + irecv, etc)

            auto& gid = std::get<1>(y);
            auto& ds  = std::get<0>(y);

            if (file_space.intersects(ds) && blocks.find(gid) == blocks.end())
            {
                blocks.insert(gid);
                send(intercomm(intercomm_index), gid, tags::consumer, msgs::data, id, file_space);

                diy::MemoryBuffer queue;
                int msg = recv(intercomm(intercomm_index), gid, tags::producer, queue); expected(msg, msgs::data);

                while (queue)
                {
                    Dataspace ds;
                    diy::load(queue, ds);

                    if (!file_space.intersects(ds))
                        throw MetadataError(fmt::format("Error: query(): received dataspace {}\ndoes not intersect file space {}\n", ds, file_space));

                    Dataspace mem_dst(Dataspace::project_intersection(file_space.id, mem_space.id, ds.id), true);
                    Dataspace::iterate(mem_dst, type.dtype_size, [&](size_t loc, size_t len)
                    {
                        std::memcpy((char*)buf + loc, queue.advance(len), len);
                    });
                }
            }
        }
    }
};

}
