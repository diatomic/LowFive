#pragma once

#include "index-query.hpp"


namespace LowFive
{

struct Index: public IndexQuery
{
    using ServeData             = std::vector<Dataset*>;        // datasets producer is serving

    BoxLocations            boxes;      // boxes we are responsible for under the regular decomposition (for redirects)
    const ServeData*        index_data; // local data for multiple datasets

    int                         dim;
    Datatype                    type;
    Decomposer                  decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy, overwritten in the constructor
    std::string                 serve_dset_path;                // full path of next dataset to serve

    // producer version of the constructor
                        Index(communicator& local_, communicator& intercomm_, const ServeData& serve_data):
                            IndexQuery(local_, intercomm_)
    {
        // TODO: assuming all datasets have same size, space, type
        Dataset* dataset = serve_data[0];

        dim = dataset->space.dims.size();
        type = dataset->type;

        Bounds domain { dim };
        domain.max = dataset->space.dims;

        decomposer = Decomposer(dim, domain, local.size());

        index(serve_data);
    }

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous
    // NB, index and query are called once for each (DIY) block of the user code, not once per rank
    // The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
    // It also means that all ranks in a producer-consumer task pair need to index/query the same number of times
    void                index(const ServeData& serve_data)
    {
        // helper for rexchange, no other purpose
        diy::Master master(local, 1, -1);
        int x = 1;      // dummy, to store non-null pointer, so master doesn't skip the block
        master.add(local.rank(), &x, new Link);

        // enqueue all file dataspaces available on local rank to the ranks
        // that are responsible for the boxes that (might) intersect them
        master.foreach([&](void*, const diy::Master::ProxyWithLink& cp)
        {
            index_data = &serve_data;
            // TODO: assuming filespace is same for all datasets
            Dataset* dset = (*index_data)[0];

            for (auto& x : dset->data)
            {
                Bounds b { dim };           // diy representation of the triplet's bounding box
                b.min = Point(x.file.min);
                b.max = Point(x.file.max);
                for (int gid : bounds_to_gids(b, decomposer))
                    cp.enqueue({ gid, gid }, x.file);
            }
        });

        master.exchange(true);      // rexchange

        // dequeue bounds and save them in boxes
        master.foreach([&](void*, const diy::Master::ProxyWithLink& cp)
        {
            for (auto& x : *cp.incoming())
            {
                int     gid     = x.first;
                auto&   queue   = x.second;
                while (queue)
                {
                    Dataspace ds(0);            // dummy to be filled
                    cp.dequeue(gid, ds);
                    boxes.emplace_back(ds, gid);
                }
            }
        });
    }

    void                serve()
    {
        local.barrier();
        if (local.rank() == 0)
        {
            // signal to consumer that we are ready
            send(intercomm, 0, tags::producer, msgs::ready, 0);
        }

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
                Dataspace ds(0);               // dummy to be filled
                diy::load(b, ds);

                BoxLocations redirects;
                for (auto& y : boxes)
                {
                    auto& ds2 = std::get<0>(y);
                    if (ds.intersects(ds2))
                        redirects.push_back(y);
                }

                send(intercomm, source, tags::producer, msgs::redirect, redirects);
            } else if (msg == msgs::dataset_path)
            {
                diy::load(b, serve_dset_path);
            } else if (msg == msgs::data)
            {
                Dataspace ds;
                diy::load(b, ds);

                diy::MemoryBuffer queue;

                for (auto& dset : *index_data)
                {
                    // serve the dataset if the full path matches the last queried path name
                    std::string full_path, unused;
                    MetadataVOL::backtrack_name(dset->name, dset->parent, unused, full_path);
                    if (full_path == serve_dset_path)
                    {
                        for (auto& y : dset->data)
                        {
                            if (y.file.intersects(ds))
                            {
                                Dataspace file_src(Dataspace::project_intersection(y.file.id, y.file.id,   ds.id), true);
                                Dataspace mem_src (Dataspace::project_intersection(y.file.id, y.memory.id, ds.id), true);
                                diy::save(queue, file_src);
                                Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                                        {
                                        diy::save(queue, (char*) y.data + loc, len);
                                        });
                            }
                        }
                        send(intercomm, source, tags::producer, msgs::data, queue);     // append msgs::data and send
                    }
                }
            } else if (msg == msgs::done)
            {
                all_done = local.ibarrier();
                all_done_active = true;
            }

            // TODO: add other potential queries (e.g., datatype)
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
            print(local.rank(), boxes);
    }
};

}
