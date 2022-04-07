#pragma once

#include "index-query.hpp"


namespace LowFive
{

struct Index: public IndexQuery
{
    struct IndexedDataset
    {
        IndexedDataset(Dataset* ds_, int comm_size):
            ds(ds_)
        {
            dim = ds->space.dims.size();
            type = ds->type;
            space = ds->space;

            Bounds domain { dim };
            domain.max = ds->space.dims;

            decomposer = Decomposer(dim, domain, comm_size);
        }

        Dataset*                ds;
        int                     dim;
        Datatype                type;
        Dataspace               space;
        Decomposer              decomposer { 1, Bounds { { 0 }, { 1} }, 1 };
        BoxLocations            boxes;
    };
    using IndexedDatasets       = std::map<std::string, IndexedDataset>;
    using IDsMap                = std::map<std::string, int>;
    using IDsVector             = std::vector<std::string>;
    using ServeData             = std::vector<Dataset*>;            // datasets producer is serving

    IndexedDatasets             index_data; // local data for multiple datasets
    IDsMap                      ids_map;
    IDsVector                   ids_vector;

    // producer version of the constructor
                        Index(communicator& local_, communicators& intercomms_, const ServeData& serve_data):
                            IndexQuery(local_, intercomms_)
    {
        // TODO: sort serve_data by name, to make sure the order is the same on all ranks

        int id = 0;
        for (auto* ds : serve_data)
        {
            std::string filename, name;
            std::tie(filename,name) = ds->fullname();
            auto it = index_data.emplace(name, IndexedDataset(ds, local.size())).first;

            ids_map[name] = id++;
            ids_vector.push_back(name);

            index(it->second);
        }
    }

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous
    // NB, index and query are called once for each (DIY) block of the user code, not once per rank
    // The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
    // It also means that all ranks in a producer-consumer task pair need to index/query the same number of times
    void                index(IndexedDataset& data)
    {
        // helper for rexchange, no other purpose
        diy::Master master(local, 1, -1);
        int x = 1;      // dummy, to store non-null pointer, so master doesn't skip the block
        master.add(local.rank(), &x, new Link);

        // enqueue all file dataspaces available on local rank to the ranks
        // that are responsible for the boxes that (might) intersect them
        master.foreach([&](void*, const diy::Master::ProxyWithLink& cp)
        {
            Dataset* dset = data.ds;

            for (auto& x : dset->data)
            {
                Bounds b { data.dim };           // diy representation of the triplet's bounding box
                b.min = Point(x.file.min);
                b.max = Point(x.file.max);
                for (int gid : bounds_to_gids(b, data.decomposer))
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
                    data.boxes.emplace_back(ds, gid);
                }
            }
        });
    }

    void                serve()
    {
        local.barrier();
        //if (local.rank() == 0)
        //{
        //    // signal to consumer that we are ready
        //    send(intercomm, 0, tags::producer, msgs::ready, 0);
        //}

        int open_files = 0;

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

            bool found_request = false;
            communicator* p_intercomm = nullptr;
            diy::mpi::optional<diy::mpi::status> ostatus;
            for (auto& intercomm : intercomms)
            {
                ostatus = intercomm.iprobe(diy::mpi::any_source, tags::consumer);
                if (ostatus)
                {
                    found_request = true;
                    p_intercomm = &intercomm;
                    break;
                }
            }
            if (!found_request)
                continue;

            communicator& intercomm = *p_intercomm;

            int source = ostatus->source();

            diy::MemoryBuffer b;
            int msg = recv(intercomm, source, tags::consumer, b);

            if (msg == msgs::file)
            {
                bool open = false;
                diy::load(b, open);
                if (open)
                    open_files++;
                else
                    open_files--;

                // NB: this will only get triggered after a file has been open;
                //     might need to tweak this, if need to make sure multiple files have been opened
                if (open_files == 0)
                {
                    all_done = local.ibarrier();
                    all_done_active = true;
                }

                continue;
            }

            if (msg == msgs::id)
            {
                std::string name;
                diy::load(b, name);
                send(intercomm, source, tags::producer, msgs::id, ids_map[name]);
                continue;
            }

            int id;
            diy::load(b, id);

            IndexedDataset& data = index_data.find(ids_vector[id])->second;

            if (msg == msgs::dimension)
            {
                diy::MemoryBuffer out;
                send(intercomm, source, tags::producer, msgs::dimension, data.dim);
                send(intercomm, source, tags::producer, msgs::dimension, data.type);
                send(intercomm, source, tags::producer, msgs::dimension, data.space);
            } else if (msg == msgs::domain)
            {
                send(intercomm, source, tags::producer, msgs::domain, data.decomposer.domain);
            } else if (msg == msgs::redirect)
            {
                Dataspace ds(0);               // dummy to be filled
                diy::load(b, ds);

                BoxLocations redirects;
                for (auto& y : data.boxes)
                {
                    auto& ds2 = std::get<0>(y);
                    if (ds.intersects(ds2))
                        redirects.push_back(y);
                }

                send(intercomm, source, tags::producer, msgs::redirect, redirects);
            } else if (msg == msgs::data)
            {
                Dataspace ds;
                diy::load(b, ds);

                diy::MemoryBuffer queue;

                for (auto& y : data.ds->data)
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
        for (auto& x : index_data)
        {
            auto& data = x.second;
            fmt::print("{}: {}\n", local.rank(), data.ds->name);
            print(local.rank(), data.boxes);
        }
    }
};

}
