#pragma once

#include <diy/decomposition.hpp>
#include <diy/types.hpp>

#include <lowfive/metadata.hpp>

struct Index
{
    using Bounds                = diy::DiscreteBounds;
    using Point                 = Bounds::Point;
    using Coordinate            = int;
    using Decomposer            = diy::RegularDecomposer<diy::DiscreteBounds>;
    using DivisionsVector       = Decomposer::DivisionsVector;
    using BlockID               = diy::BlockID;
    using Link                  = diy::RegularGridLink;

    using BoxLocations          = std::vector<std::tuple<LowFive::Dataspace, int>>;

    using communicator          = diy::mpi::communicator;

    struct Block
    {
        BoxLocations                            boxes;      // boxes we are responsible for under the regular decomposition (for redirects)
        const LowFive::Dataset::DataTriples*    data;       // local data
    };

    enum msgs   { dimension = 1, domain, redirect, data, done };        // message type

    // tags indicate the source of communication, so that in the threaded
    // regime, they can be used to correctly distinguish between senders and
    // receipients
    enum tags   {
                    producer = 1,       // communication from producer
                    consumer            // communication from consumer
                };

    // producer version of the constructor
                        Index(communicator& local_, communicator& intercomm_, const LowFive::Dataset* dataset):
                            local(local_),
                            intercomm(intercomm_),
                            master(local, 1, local.size()),
                            assigner(local.size(), local.size())
    {
        dim = dataset->space.dims.size();
        type = dataset->type;

        Bounds domain { dim };
        domain.max = dataset->space.dims;

        decomposer = Decomposer(dim, domain, local.size());
        decomposer.decompose(local.rank(), assigner,
                             [&](int            gid,            // block global id
                                 const  Bounds& core,           // block bounds without any ghost added
                                 const  Bounds& bounds,         // block bounds including any ghost region added
                                 const  Bounds& domain,         // global data bounds
                                 const  Link&   link)           // neighborhood
        {
            Block*          b   = new Block;
            Link*           l   = new Link(link);

            master.add(gid, b, l);
        });

        index(dataset->data);
    }

    // consumer version of the constructor
                        Index(communicator& local_, communicator& intercomm_):
                            local(local_),
                            intercomm(intercomm_),
                            master(local, 1, local.size()),                     // unused, but must initialize
                            assigner(intercomm.size(), intercomm.size())        // producer info; TODO: this should be remote_size, not convenient to fix in the constructor
    {
        bool root = local.rank() == 0;

        // query producer for dim
        if (root)
        {
            send(intercomm, 0, tags::consumer, msgs::dimension, 0);
            int msg = recv(intercomm, 0, tags::producer, dim);  expected(msg, msgs::dimension);
                msg = recv(intercomm, 0, tags::producer, type); expected(msg, msgs::dimension);
        }
        diy::mpi::broadcast(local, dim, 0);
        broadcast(local, type, 0);

        // query producer for domain
        Bounds domain { dim };
        if (root)
        {
            send(intercomm, 0, tags::consumer, msgs::domain, 0);
            int msg = recv(intercomm, 0, tags::producer, domain);
            expected(msg, msgs::domain);
        }
        broadcast(local, domain, 0);

        int remote_size;
        MPI_Comm_remote_size(intercomm, &remote_size);
        decomposer = Decomposer(dim, domain, remote_size);
    }

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous
    // NB, index and query are called once for each (DIY) block of the user code, not once per rank
    // The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
    // It also means that all ranks in a producer-consumer task pair need to index/query the same number of times

    void                index(const LowFive::Dataset::DataTriples& data)
    {
        // enqueue all file dataspaces available on local rank to the ranks
        // that are responsible for the boxes that (might) intersect them
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            b->data = &data;
            for (auto& x : data)
            {
                Bounds b { dim };           // diy representation of the triplet's bounding box
                b.min = Point(x.file.min);
                b.max = Point(x.file.max);
                for (int gid : bounds_to_gids(b))
                    cp.enqueue({ gid, assigner.rank(gid) }, x.file);
            }
        });

        master.exchange(true);      // rexchange

        // dequeue bounds and save them in boxes
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            for (auto& x : *cp.incoming())
            {
                int     gid     = x.first;
                auto&   queue   = x.second;
                while (queue)
                {
                    LowFive::Dataspace ds(0);            // dummy to be filled
                    cp.dequeue(gid, ds);
                    b->boxes.emplace_back(ds, gid);
                }
            }
        });
    }

    // serialize and send
    template<class T>
    static void         send(communicator& comm, int dest, int tag, int msg, const T& x)
    {
        diy::MemoryBuffer b;
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

    void                serve()
    {
        diy::mpi::request all_done;
        bool all_done_active = false;
        if (local.rank() != 0)
        {
            all_done = local.ibarrier();
            all_done_active = true;
        }

        while (true)
        {
            if (all_done_active && all_done.test())
                break;

            diy::mpi::optional<diy::mpi::status> ostatus = intercomm.iprobe(diy::mpi::any_source, tags::consumer);
            if (!ostatus)
                continue;

            int tag    = ostatus->tag();
            int source = ostatus->source();

            diy::MemoryBuffer b;
            int msg = recv(intercomm, source, tags::consumer, b);

            if (msg == msgs::dimension)
            {
                diy::MemoryBuffer out;
                send(intercomm, source, tags::producer, msgs::dimension, dim);
                send(intercomm, source, tags::producer, msgs::dimension, type);
            } else if (msg == msgs::domain)
            {
                send(intercomm, source, tags::producer, msgs::domain, decomposer.domain);
            } else if (msg == msgs::redirect)
            {
                LowFive::Dataspace ds(0);               // dummy to be filled
                diy::load(b, ds);

                auto* b = master.block<Block>(0);       // only one block per rank
                BoxLocations redirects;
                for (auto& y : b->boxes)
                {
                    auto& ds2 = std::get<0>(y);
                    if (ds.intersects(ds2))
                        redirects.push_back(y);
                }

                send(intercomm, source, tags::producer, msgs::redirect, redirects);
            } else if (msg == msgs::data)
            {
                LowFive::Dataspace ds;
                diy::load(b, ds);

                auto* b = master.block<Block>(0);       // only one block per rank
                diy::MemoryBuffer queue;
                for (auto& y : *(b->data))
                {
                    if (y.file.intersects(ds))
                    {
                        LowFive::Dataspace file_src(LowFive::Dataspace::project_intersection(y.file.id, y.file.id,   ds.id), true);
                        LowFive::Dataspace mem_src (LowFive::Dataspace::project_intersection(y.file.id, y.memory.id, ds.id), true);
                        diy::save(queue, file_src);
                        LowFive::Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                        {
                            diy::save(queue, (char*) y.data + loc, len);
                        });
                    }
                }
                send(intercomm, source, tags::producer, msgs::data, queue);     // append msgs::data and send
            } else if (msg == msgs::done)
            {
                all_done = local.ibarrier();
                all_done_active = true;
            }

            // TODO: add other potential queries (e.g., datatype)
        }
    }

    static void         expected(int received_, int expected_)
    {
        if (received_ != expected_)
            throw std::runtime_error(fmt::format("Message mismatch: expected = {}, received {}", expected_, received_));
    }

    void                close()
    {
        local.barrier();

        if (local.rank() == 0)
            send(intercomm, 0, tags::consumer, msgs::done, 0);
    }

    void                query(const LowFive::Dataspace&            file_space,      // input: query in terms of file space
                              const LowFive::Dataspace&            mem_space,       // ouput: memory space of resulting data
                              void*                                buf)             // output: resulting data, allocated by caller
    {
        // enqueue queried file dataspace to the ranks that are
        // responsible for the boxes that (might) intersect them
        Bounds b { dim };           // diy representation of the dataspace's bounding box
        b.min = Point(file_space.min);
        b.max = Point(file_space.max);

        BoxLocations all_redirects;
        auto gids = bounds_to_gids(b);
        for (int gid : bounds_to_gids(b))
        {
            // TODO: make this asynchronous (isend + irecv, etc)
            send(intercomm, gid, tags::consumer, msgs::redirect, file_space);

            BoxLocations redirects;
            int msg = recv(intercomm, gid, tags::producer, redirects); expected(msg, msgs::redirect);
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
                send(intercomm, gid, tags::consumer, msgs::data, file_space);

                diy::MemoryBuffer queue;
                int msg = recv(intercomm, gid, tags::producer, queue); expected(msg, msgs::data);

                while (queue)
                {
                    LowFive::Dataspace ds;
                    diy::load(queue, ds);

                    if (!file_space.intersects(ds))
                        throw LowFive::MetadataError(fmt::format("Error: query(): received dataspace {}\ndoes not intersect file space {}\n", ds, file_space));

                    LowFive::Dataspace mem_dst(LowFive::Dataspace::project_intersection(file_space.id, mem_space.id, ds.id), true);
                    LowFive::Dataspace::iterate(mem_dst, type.dtype_size, [&](size_t loc, size_t len)
                    {
                        std::memcpy((char*)buf + loc, queue.advance(len), len);
                    });
                }
            }
        }
    }

    static void         print(int rank, const BoxLocations& boxes)
    {
        for (auto& box : boxes)
        {
            auto& gid = std::get<1>(box);
            auto& ds  = std::get<0>(box);
            fmt::print("{}: ({}) -> {}\n", rank, gid, ds);
        }
    }

    void                print()
    {
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            if (master.communicator().rank() == 0)
                print(master.communicator().rank(), b->boxes);

            // debug: print the data
//             for (auto& x : *(b->data))
//             {
//                 if (master.communicator().rank() == 0)
//                     fmt::print("index has data ranging from {} to {}\n",
//                         ((float*)x.data)[0], ((float*)x.data)[x.memory.size() - 1]);
//             }
        });
    }

    communicator                local, intercomm;
    diy::Master                 master;
    diy::ContiguousAssigner     assigner;
    int                         dim;
    LowFive::Datatype           type;
    Decomposer                  decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy, overwritten in the constructor


    // TODO: move this into RegularDecomposer itself (this is a slightly modified point_to_gids)
    std::vector<int>    bounds_to_gids(const Bounds& bounds) const
    {
        std::vector< std::pair<int, int> > ranges(dim);
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
                bottom  = std::max(0, bottom);
                top     = std::min(decomposer.divisions[i], top);
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
            unsigned i = 0;
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
