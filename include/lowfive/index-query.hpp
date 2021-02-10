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

    using BoxLocations          = std::vector<std::tuple<LowFive::Dataspace, BlockID>>;

    struct Block
    {
        BoxLocations                            boxes;      // boxes we are responsible for under the regular decomposition (for redirects)
        const LowFive::Dataset::DataTriples*    data;       // local data
    };

                        Index(diy::mpi::communicator& world, const LowFive::Dataset* dataset):
                            master(world, 1, world.size()),
                            assigner(world.size(), world.size())
    {
        dim = dataset->space.dims.size();
        Bounds domain { dim };
        domain.max = dataset->space.dims;

        decomposer = Decomposer(dim, domain, world.size());
        decomposer.decompose(world.rank(), assigner,
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

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous

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
                    b->boxes.emplace_back(ds, BlockID { gid, assigner.rank(gid) });
                }
            }
        });
    }

    void                query(const LowFive::Dataset&              dataset,         // input: dataset
                              const LowFive::Dataspace&            file_space,      // input: query in terms of file space
                              const LowFive::Dataspace&            mem_space,       // ouput: memory space of resulting data
                              void*                                buf)             // output: resulting data, allocated by caller
    {
//         fmt::print("Querying\n");

        const LowFive::Dataset::DataTriples& data = dataset.data;

        // enqueue queried file dataspace to the ranks that are
        // responsible for the boxes that (might) intersect them
        master.foreach([&](Block*, const diy::Master::ProxyWithLink& cp)
        {
            Bounds b { dim };           // diy representation of the dataspace's bounding box
            b.min = Point(file_space.min);
            b.max = Point(file_space.max);
            auto gids = bounds_to_gids(b);
            for (int gid : bounds_to_gids(b))
                cp.enqueue({ gid, assigner.rank(gid) }, file_space);
        });
        master.exchange(true);      // rexchange

        // dequeue and check intersections
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

                    BoxLocations redirects;
                    for (auto& y : b->boxes)
                    {
                        auto& ds2 = std::get<0>(y);
                        if (ds.intersects(ds2))
                            redirects.push_back(y);
                    }

                    cp.enqueue(BlockID { gid, assigner.rank(gid) }, redirects);
                }
            }
        });
        master.exchange(true);      // rexchange

        // dequeue redirects and request data;
        //fmt::print("Redirects:\n");
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            // dequeue redirects
            BoxLocations all_redirects;
            for (auto& x : *cp.incoming())
            {
                int     gid     = x.first;
                auto&   queue   = x.second;
                while (queue)
                {
                    BoxLocations redirects;
                    cp.dequeue(gid, redirects);
                    for (auto& x : redirects)
                        all_redirects.push_back(x);
                }
            }
            //print(master.communicator().rank(), all_redirects);

            // request data for the queried space
            std::set<BlockID> blocks;
            for (auto& y : all_redirects)
            {
                auto& bid = std::get<1>(y);
                auto& ds  = std::get<0>(y);

                if (file_space.intersects(ds) && blocks.find(bid) == blocks.end())
                {
                    cp.enqueue(bid, file_space);
                    blocks.insert(bid);
                }
            }
        });
        master.exchange(true);

        // send the data for the queried space
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            for (auto& x : *cp.incoming())
            {
                int     gid     = x.first;
                BlockID bid { gid, assigner.rank(gid) };

                auto&   queue   = x.second;
                while (queue)
                {
                    LowFive::Dataspace ds;
                    cp.dequeue(gid, ds);
                    for (auto& y : *(b->data))
                    {
                        if (y.file.intersects(ds))
                        {
                            LowFive::Dataspace file_src(LowFive::Dataspace::project_intersection(y.file.id, y.file.id,   ds.id), true);
                            LowFive::Dataspace mem_src (LowFive::Dataspace::project_intersection(y.file.id, y.memory.id, ds.id), true);
                            cp.enqueue(bid, file_src);
                            LowFive::Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                            {
                                cp.enqueue(bid, (char*) y.data + loc, len);
                            });
                        }
                    }
                }
            }
        });
        master.exchange(true);

        // receive the data for the queried space
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            for (auto& x : *cp.incoming())
            {
                int     gid     = x.first;
                BlockID bid { gid, assigner.rank(gid) };

                auto&   queue   = x.second;
                while (queue)
                {
                    LowFive::Dataspace ds;
                    cp.dequeue(gid, ds);

                    if (!file_space.intersects(ds))
                        throw LowFive::MetadataError(fmt::format("Error: query(): received dataspace {}\ndoes not intersect file space {}\n", ds, file_space));

                    LowFive::Dataspace mem_dst(LowFive::Dataspace::project_intersection(file_space.id, mem_space.id, ds.id), true);
                    LowFive::Dataspace::iterate(mem_dst, data[0].type.dtype_size, [&](size_t loc, size_t len)
                    {
                        std::memcpy((char*)buf + loc, queue.advance(len), len);
                    });
                }
            }
        });
    }

    static void         print(int rank, const BoxLocations& boxes)
    {
        for (auto& box : boxes)
        {
            auto& bid = std::get<1>(box);
            auto& ds  = std::get<0>(box);
            fmt::print("{}: ({} {}) -> {}\n", rank, bid.gid, bid.proc, ds);
        }
    }

    void                print()
    {
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            print(master.communicator().rank(), b->boxes);

            // debug: print the data
            for (auto& x : *(b->data))
            {
                fmt::print("index has data ranging from {} to {}\n",
                        ((float*)x.data)[0], ((float*)x.data)[x.memory.size() - 1]);
            }
        });
    }

    diy::Master                 master;
    diy::ContiguousAssigner     assigner;
    int                         dim;
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
