#include "../query.hpp"
#include <lowfive/log.hpp>
#include <diy/types.hpp>
#include <diy/decomposition.hpp>

#include "../log-private.hpp"

#include "core.hpp"


namespace LowFive {

Query::Query(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, int remote_size_, int intercomm_index_):
        IndexQuery(local_, intercomms_),
        remote_size(remote_size_),
        intercomm_index(intercomm_index_),
        c(m, intercomms_[intercomm_index_], local.rank() % remote_size)
{
    export_core(m, nullptr);        // client doesn't need IndexServe
}

void
RemoteDataset::
query(const Dataspace&  file_space,      // input: query in terms of file space
      const Dataspace&  mem_space,       // ouput: memory space of resulting data
      void*             buf)             // output: resulting data, allocated by caller
{
    using Bounds        = IndexQuery::Bounds;
    using Point         = IndexQuery::Point;
    using BoxLocations  = IndexQuery::BoxLocations;

    auto log = get_logger();
    log->trace("RemoteDataset::query: file_space = {}", file_space);

    // enqueue queried file dataspace to the ranks that are
    // responsible for the boxes that (might) intersect them
    Bounds b { dim };           // diy representation of the dataspace's bounding box
    b.min = Point(file_space.min);
    b.max = Point(file_space.max);

    BoxLocations all_redirects;
    auto gids = IndexQuery::bounds_to_gids(b, decomposer);
    for (int gid : gids)
    {
        // TODO: make this asynchronous (isend + irecv, etc)

        // TODO: keep these open for the next loop
        auto rids = obj.self_->call<rpc::client::object>(gid, "open_indexed_dataset", fullname());
        BoxLocations redirects = rids.call<BoxLocations>("redirects", file_space);
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
        log->trace("Processing redirect: gid = {}, ds = {}", gid, ds);

        if (file_space.intersects(ds) && blocks.find(gid) == blocks.end())
        {
            blocks.insert(gid);

            // open the object on the right rank
            auto rids = obj.self_->call<rpc::client::object>(gid, "open_indexed_dataset", fullname());
            log->trace("Opened object: {} (own = {})", rids.id_, rids.own_);
            log->trace("Opened dataset on {}", gid);

            auto queue = rids.call<diy::MemoryBuffer>("get_data", file_space);
            queue.reset();      // move position to 0
            log->trace("Received queue of size: {}", queue.size());

            while (queue)
            {
                Dataspace ds;
                diy::load(queue, ds);

                log->trace("Received {} to requested {}", ds, file_space);

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
    log->trace("Leaving RemoteDataset::query");
}


DistMetadataVOL::FileNames
Query::get_filenames()
{
    // vector of std::string for now
    using FileNames = DistMetadataVOL::FileNames;

    auto log = get_logger();

    log->trace("Enter Query::get_filenames()");

    FileNames file_names;

    bool root = local.rank() == 0;

    if (root)
        file_names = c.call<std::vector<std::string>>("get_filenames");

    // broadcast file_names to all ranks from root

    auto n_file_names = file_names.size();
    diy::mpi::broadcast(local, n_file_names,  0);
    log->trace("Query::get_filenames() broadcast n_file_names = {}", n_file_names);

    if (!root)
        file_names = FileNames(n_file_names, "");

    // FIXME: this is a terrible way to do this
    for(size_t i = 0; i < n_file_names; ++i) {
        broadcast(local, file_names[i], 0);
    }

    log->trace("Exit Query::get_filenames()");
    return file_names;
}

void
Query::send_done()
{
    auto log = get_logger();
    log->trace("Enter Query::send_done()");

    bool root = local.rank() == 0;
    if (root)
        c.finish(0);
}


} // namespace LowFive
