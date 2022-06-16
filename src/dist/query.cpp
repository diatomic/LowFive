#include "../query.hpp"
#include <lowfive/log.hpp>
#include <diy/types.hpp>
#include <diy/decomposition.hpp>

#include "../log-private.hpp"

namespace LowFive {

Query::Query(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, int remote_size_, int intercomm_index_):
        IndexQuery(local_, intercomms_),
        remote_size(remote_size_),
        intercomm_index(intercomm_index_)
{}

void
Query::file_open()
{
    bool root = local.rank() == 0;
    if (root)
    {
        send(intercomm(intercomm_index), 0, tags::consumer, msgs::file, true);
    }
}

void
Query::file_close()
{
    local.barrier();

    bool root = local.rank() == 0;
    if (root)
    {
        send(intercomm(intercomm_index), 0, tags::consumer, msgs::file, false);
    }
}

void Query::dataset_open(std::string name)
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

void
Query::dataset_close()
{
}

void
Query::query(const Dataspace&  file_space,      // input: query in terms of file space
             const Dataspace&  mem_space,       // ouput: memory space of resulting data
             void*             buf)             // output: resulting data, allocated by caller
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


DistMetadataVOL::FileNames
Query::get_filenames()
{
    // vector of std::string for now
    using FileNames = DistMetadataVOL::FileNames;

    auto log = get_logger();

    log->trace("Enter Query::get_filenames()");

    FileNames file_names;
    size_t n_file_names {0};

    bool root = local.rank() == 0;

    if (root)
    {
        send(intercomm(intercomm_index), 0, tags::consumer, msgs::fnames, true);
        log->trace("Query::get_filenames() sent msgs::fnames");

        diy::MemoryBuffer queue;
        int msg = recv(intercomm(intercomm_index), 0, tags::producer, queue);
        log->trace("Query::get_filenames() recv done");
        expected(msg, msgs::fnames);

        if(queue)
        {
            diy::load(queue, n_file_names);
            log->trace("Query::get_filenames() loaded n_fnames = {}", n_file_names);
            file_names = FileNames(n_file_names, "");
            for(size_t i = 0; i < n_file_names; ++i) {
                diy::load(queue, file_names[i]);
            }
        } else
        {
            log->trace("Query::get_filenames() queue is empty");
        }
    }

    // broadcast file_names to all ranks from root

    diy::mpi::broadcast(local, n_file_names,  0);
    log->trace("Query::get_filenames() broadcast n_file_names = {}", n_file_names);

    if (!root)
        file_names = FileNames(n_file_names, "");

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
    if (root) {
        send(intercomm(intercomm_index), 0, tags::consumer, msgs::done, true);
        log->trace("Query::send_done(): sent msgs::done to intercomm index = {}", intercomm_index);
    }
}


} // namespace LowFive