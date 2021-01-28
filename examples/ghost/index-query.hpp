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
        BoxLocations boxes;
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

    // index-query are written with the bulk-synchronous assumption;
    // think about how to make it completely asynchronous

    void                index(const LowFive::Dataset::DataTriples& data)
    {
        // enqueue all file dataspaces available on local rank to the ranks
        // that are responsible for the boxes that (might) intersect them
        master.foreach([&](Block*, const diy::Master::ProxyWithLink& cp)
        {
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

    void                query(const LowFive::Dataset::DataTriples& data)
    {
        // enqueue all queried file dataspaces to the ranks that are
        // responsible for the boxes that (might) intersect them
        master.foreach([&](Block*, const diy::Master::ProxyWithLink& cp)
        {
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

        // dequeue redirects and request data
    }

    void                print()
    {
        master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
        {
            for (auto& box : b->boxes)
            {
                auto& bid = std::get<1>(box);
                auto& ds  = std::get<0>(box);
                fmt::print("{}: ({} {}) -> {}\n", master.communicator().rank(), bid.gid, bid.proc, ds);
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
