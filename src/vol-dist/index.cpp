#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/log.hpp>
#include "../index.hpp"
#include "../log-private.hpp"

#include "core.hpp"

namespace LowFive {

// producer version of the constructor
Index::Index(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, Files* files):
    IndexQuery(local_, intercomms_), idx_srv(files)
{
    auto log = get_logger();
    log->trace("Index ctor, number of intercomms: {}, files.size = {}", intercomms_.size(), files->size());

    // traverse all datasets
    for (auto& f : *(idx_srv.files))
    {
        auto datasets = find_datasets(dynamic_cast<File*>(f.second));
        for (auto& x : datasets)
        {
            auto* ds = x.second;
            IndexedDataset* ids = new IndexedDataset(ds, IndexQuery::local.size());
            index(*ids);
            ds->extra = ids;

            ++indexed_datasets;
        }
    }
}

Index::~Index()
{
    // TODO: traverse and destroy all IndexedData
    for (auto& f : *(idx_srv.files))
    {
        auto datasets = find_datasets(dynamic_cast<File*>(f.second));
        for (auto& x : datasets)
        {
            auto* ds = x.second;
            IndexedDataset* ids = (IndexedDataset*) ds->extra;
            delete ids;
            ds->extra = nullptr;
        }
    }
}

// TODO: index-query are written with the bulk-synchronous assumption;
//       think about how to make it completely asynchronous
// NB, index and query are called once for each (DIY) block of the user code, not once per rank
// The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
// It also means that all ranks in a producer-consumer task pair need to index/query the same number of times
void
Index::index(IndexedDataset& data)
{
    auto log = get_logger();
    log->trace("Enter Index::index");

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

    log->trace("Exit Index::index");
}

void
Index::serve()
{
    auto log = get_logger();
    log->trace("Enter Index::serve");
    local.barrier();

    bool root = (local.rank() == 0);

    //if (local.rank() == 0)
    //{
    //    // signal to consumer that we are ready
    //    send(intercomm, 0, tags::producer, msgs::ready, 0);
    //}

    diy::mpi::request all_done;
    bool all_done_active = false;
    if (!root)
    {
        all_done = local.ibarrier();
        all_done_active = true;
    }

    std::vector<std::unique_ptr<rpc::server::module>>   modules;
    std::vector<rpc::server>                            servers;

    for (auto& intercom : intercomms)
    {
        modules.emplace_back(new rpc::server::module);
        export_core(*(modules.back()), &idx_srv);

        servers.emplace_back(*(modules.back()), intercom);
    }

    size_t all_done_count = 0;
    while (true)
    {
        if (all_done_active && all_done.test())
            break;

        for (auto& s : servers)
        {
            int source = s.probe();
            if (source >= 0)
            {
                if (s.process(source))   // true indicates that finish was called
                {
                    all_done_count++;

                    if (all_done_count == servers.size())
                    {
                        all_done = local.ibarrier();
                        all_done_active = true;
                    }
                }
            }
        }
    }
    if (!idx_srv.done)
        log->warn("Not all files have been closed (this is Ok, if no files were opened, e.g., when producer signals that it's done)");

    log->trace("Done with Index::serve");
}

void
Index::print(int rank, const BoxLocations& boxes)
{
    for (auto& box : boxes)
    {
        auto& gid = std::get<1>(box);
        auto& ds  = std::get<0>(box);
        fmt::print("{}: ({}) -> {}\n", rank, gid, ds);
    }
}

Index::Datasets
Index::
find_datasets(File* f)
{
    Datasets result;
    std::string name;
    find_datasets(f, name, result);
    return result;
}

void
Index::
find_datasets(Object* o, std::string name, Datasets& result)
{
    name += "/" + o->name;
    Dataset* ds = dynamic_cast<Dataset*>(o);
    if (ds)
        result.emplace(name, ds);

    for (Object* child : o->children)
        find_datasets(child, name, result);
}

} // namespace LowFive
