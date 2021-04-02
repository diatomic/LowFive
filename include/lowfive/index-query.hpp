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

    enum tags { dimension = 1, domain, redirect, data, done };

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
                            assigner(intercomm.size(), intercomm.size())        // producer info
    {
        bool root = local.rank() == 0;

        // query producer for dim
        if (root)
        {
            intercomm.send(0, tags::dimension, 0);
            intercomm.recv(0, tags::dimension, dim);
            recv(intercomm, 0, tags::dimension, type);
        }
        diy::mpi::broadcast(local, dim, 0);
        broadcast(local, type, 0);

        // query producer for domain
        Bounds domain { dim };
        if (root)
        {
            intercomm.send(0, tags::domain, 0);
            recv(intercomm, 0, tags::domain, domain);
        }
        broadcast(local, domain, 0);

        decomposer = Decomposer(dim, domain, intercomm.size());
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
    static void         send(communicator& comm, int dest, int tag, const T& x)
    {
        diy::MemoryBuffer b;
        diy::save(b, x);
        comm.send(dest, tag, b.buffer);
    }

    // recv and deserialize
    template<class T>
    static void         recv(communicator& comm, int source, int tag, T& x)
    {
        diy::MemoryBuffer b;
        comm.recv(source, tag, b.buffer);
        diy::load(b, x);
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

            diy::mpi::optional<diy::mpi::status> ostatus = intercomm.iprobe(diy::mpi::any_source, diy::mpi::any_tag);
            if (!ostatus)
                continue;

            int tag    = ostatus->tag();
            int source = ostatus->source();

            if (tag == tags::dimension)
            {
                int x;
                intercomm.recv(source, tags::dimension, x);     // clear the message
                intercomm.send(source, tags::dimension, dim);
                send(intercomm, source, tags::dimension, type);
            } else if (tag == tags::domain)
            {
                int x;
                intercomm.recv(source, tags::domain, x);        // clear the message
                send(intercomm, source, tags::domain, decomposer.domain);
            } else if (tag == tags::redirect)
            {
                LowFive::Dataspace ds(0);               // dummy to be filled
                recv(intercomm, source, tags::redirect, ds);

                auto* b = master.block<Block>(0);       // only one block per rank
                BoxLocations redirects;
                for (auto& y : b->boxes)
                {
                    auto& ds2 = std::get<0>(y);
                    if (ds.intersects(ds2))
                        redirects.push_back(y);
                }

                send(intercomm, source, tags::redirect, redirects);
            } else if (tag == tags::data)
            {
                LowFive::Dataspace ds;
                recv(intercomm, source, tags::data, ds);

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
                intercomm.send(source, tags::data, queue.buffer);
            } else if (tag == tags::done)
            {
                int x;
                intercomm.recv(source, tags::done, x);      // clear the queue
                all_done = local.ibarrier();
                all_done_active = true;
            }

            // TODO: add other potential queries (e.g., datatype)
        }
    }

    void                close()
    {
        local.barrier();

        if (local.rank() == 0)
            intercomm.send(0, tags::done, 0);
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
            send(intercomm, gid, tags::redirect, file_space);

            BoxLocations redirects;
            recv(intercomm, gid, tags::redirect, redirects);
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
                send(intercomm, gid, tags::data, file_space);

                diy::MemoryBuffer queue;
                intercomm.recv(gid, tags::data, queue.buffer);

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
