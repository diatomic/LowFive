#include "../query.hpp"
#include <cstring>
#include <lowfive/log.hpp>
#include <diy/types.hpp>
#include <diy/decomposition.hpp>
#include <unordered_map>

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
query(const Datatype&   mem_type,
      const Dataspace&  file_space,
      const Dataspace&  mem_space,
      void*             buf)
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

    CALI_MARK_BEGIN("all_redirects");
    BoxLocations all_redirects;
    auto gids = IndexQuery::bounds_to_gids(b, decomposer);
    for (int gid : gids)
    {
        // TODO: make this asynchronous (isend + irecv, etc)

        // TODO: keep these open for the next loop
        auto rids = obj.self_->call<rpc::client::object>(gid, "open_indexed_dataset", remote_path);
        BoxLocations redirects = rids.call<BoxLocations>("redirects", file_space);
        for (auto& x : redirects)
            all_redirects.push_back(x);
    }
    CALI_MARK_END("all_redirects");

    // request and receive data
    CALI_MARK_BEGIN("request_receive_data");
    std::set<int> blocks;
    std::unordered_map<size_t, std::string> pending_strings;
    std::unordered_map<size_t, size_t> pending_offsets;
    struct PendingIndirectValues
    {
        const Datatype& type;
        std::vector<char> bytes;

        ~PendingIndirectValues()
        {
            if (!bytes.empty())
                type.destroy_values(
                    bytes.size() / type.dtype_size, bytes.data());
        }

        size_t append()
        {
            const size_t index = bytes.size() / type.dtype_size;
            bytes.resize(bytes.size() + type.dtype_size, 0);
            return index;
        }

        char* value(size_t index)
        {
            return bytes.data() + index * type.dtype_size;
        }

        void reset(size_t index)
        {
            auto* current = value(index);
            type.destroy_values(1, current);
            std::memset(current, 0, type.dtype_size);
        }
    } pending_values {type};

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
            auto rids = obj.self_->call<rpc::client::object>(
                gid, "open_indexed_dataset", remote_path);
            log->trace("Opened object: {} (own = {})", rids.id_, rids.own_);
            log->trace("Opened dataset on {}", gid);

            auto queue =
                rids.call<diy::MemoryBuffer>("get_data", file_space);
            queue.reset();      // move position to 0
            log->trace("Received queue of size: {}", queue.size());

            const bool is_var_length_string =
                type.is_var_length_string();
            const bool deserialize_indirect =
                type.has_variable_length_values();

            CALI_MARK_BEGIN("load_from_queue");
            while (queue)
            {
                Dataspace ds;
                diy::load(queue, ds);

                log->trace(
                    "Received {} to requested {}", ds, file_space);

                if (!file_space.intersects(ds))
                    throw MetadataError(fmt::format(
                        "Error: query(): received dataspace {}\n"
                        "does not intersect file space {}\n",
                        ds, file_space));

                Dataspace mem_dst(
                    Dataspace::project_intersection(
                        file_space.id, mem_space.id, ds.id),
                    true);
                if (is_var_length_string)
                {
                    Dataspace::iterate(
                        mem_dst, mem_type.dtype_size,
                        [&](size_t loc, size_t len)
                        {
                            for (size_t offset = 0;
                                 offset < len;
                                 offset += mem_type.dtype_size)
                            {
                                std::string value;
                                diy::load(queue, value);
                                pending_strings[loc + offset] =
                                    std::move(value);
                            }
                        });
                }
                else if (deserialize_indirect)
                {
                    File::References references;
                    Dataspace::iterate(
                        mem_dst, mem_type.dtype_size,
                        [&](size_t loc, size_t len)
                        {
                            for (size_t offset = 0;
                                 offset < len;
                                 offset += mem_type.dtype_size)
                            {
                                const size_t destination_offset =
                                    loc + offset;
                                auto [position, inserted] =
                                    pending_offsets.emplace(
                                        destination_offset,
                                        pending_values.bytes.size() /
                                            type.dtype_size);
                                if (inserted)
                                    position->second =
                                        pending_values.append();
                                else
                                    pending_values.reset(
                                        position->second);

                                references.clear();
                                deserialize_values(
                                    queue, type, 1,
                                    pending_values.value(
                                        position->second),
                                    &references);
                                if (!references.empty())
                                    throw MetadataError(
                                        "Distributed variable-length "
                                        "references are unsupported");
                            }
                        });
                }
                else
                {
                    Dataspace::iterate(
                        mem_dst, mem_type.dtype_size,
                        [&](size_t loc, size_t len)
                        {
                            const size_t count =
                                len / mem_type.dtype_size;
                            type.convert(
                                mem_type, count,
                                queue.advance(
                                    count * type.dtype_size),
                                static_cast<char*>(buf) + loc);
                        });
                }
            }
            CALI_MARK_END("load_from_queue");
        }
    }

    for (const auto& [offset, value] : pending_strings)
    {
        const char* source = value.empty() ? nullptr : value.c_str();
        type.convert(
            mem_type, 1, &source, static_cast<char*>(buf) + offset);
    }
    for (const auto& [offset, index] : pending_offsets)
        type.convert(
            mem_type, 1, pending_values.value(index),
            static_cast<char*>(buf) + offset);
    log->trace("Leaving RemoteDataset::query");
    CALI_MARK_END("request_receive_data");
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
