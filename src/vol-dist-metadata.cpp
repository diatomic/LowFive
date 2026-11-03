#include <lowfive/vol-dist-metadata.hpp>
#include "metadata/remote.hpp"
#include "metadata/dummy.hpp"
#include "log-private.hpp"
#include "index.hpp"
#include "query.hpp"
#include "metadata/serialization.hpp"

#include "vol-metadata-private.hpp"     // ObjectPointers
#include <stdexcept>

namespace LowFive {

RemoteFile::RemoteFile(DistMetadataVOL& vol_, const File& f,
                       rpc::client::object&& remote, std::unique_ptr<Query>&& q):
    File(f),
    RemoteObject(std::move(remote)),
    vol(vol_),
    query_(std::move(q))
{
    ++vol.remote_file_counts_[query_->intercomm_index];
    File::remote = true;
}

RemoteFile::~RemoteFile()
{
    // Child RPC objects must be released while this file still owns their client.
    auto release = [](std::vector<Object*>& objects)
    {
        for (auto* object : objects)
        {
            object->parent = nullptr;
            delete object;
        }
        objects.clear();
    };
    release(children);
    release(unlinked);

    obj.call<void>("file_close");
    MPI_Comm connection = comm != MPI_COMM_NULL ? comm : vol.local;
    MPI_Barrier(connection);

    auto session = vol.remote_file_counts_.find(query_->intercomm_index);
    assert(session != vol.remote_file_counts_.end() && session->second > 0);
    if (--session->second == 0)
    {
        vol.remote_file_counts_.erase(session);
        int rank;
        MPI_Comm_rank(connection, &rank);
        if (rank == 0)
            query_->c.finish(0);
    }

    obj.destroy();
    if (comm != MPI_COMM_NULL)
        MPI_Comm_free(&comm);
}

DistMetadataVOL::DistMetadataVOL(communicator  local_, communicator  intercomm_):
    DistMetadataVOL(local_, communicators { std::move(intercomm_) })
{
    auto log = get_logger();
    log->trace("DistMetadataVOL ctor: this = {}", fmt::ptr(this));
}

DistMetadataVOL::communicator
DistMetadataVOL::
checked_intercomm(int intercomm_index) const
{
    if (intercomm_index < 0 || static_cast<size_t>(intercomm_index) >= intercomms.size())
        throw std::out_of_range(fmt::format("Invalid intercomm index {} for {} communicators",
                                            intercomm_index, intercomms.size()));
    return intercomms[intercomm_index];
}

void
DistMetadataVOL::
set_intercomm(std::string filename, std::string full_path, int intercomm_index)
{
    checked_intercomm(intercomm_index);
    intercomm_locations.emplace_back(LocationPattern { std::move(filename), std::move(full_path) });
    intercomm_indices.emplace_back(intercomm_index);
}


int
DistMetadataVOL::
remote_size(int intercomm_index)
{
    const auto comm = checked_intercomm(intercomm_index);
    // get the remote size of the intercomm: different logic for inter- and intra-comms
    int remote_size;
    int is_inter; MPI_Comm_test_inter(comm, &is_inter);
    if (!is_inter)
        MPI_Comm_size(comm, &remote_size);
    else
        MPI_Comm_remote_size(comm, &remote_size);

    return remote_size;
}

void
DistMetadataVOL::
serve_all(bool delete_data, bool perform_indexing)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();
    log->trace("enter serve_all, files.size = {}", files.size());

    DataIntercomms indices(intercomms.size());

    if (serve_indices)
        indices = serve_indices();
    else
        std::iota(indices.begin(), indices.end(), 0);

    std::vector<MPI_Comm> selected_intercomms;
    for (auto idx : indices)
        selected_intercomms.push_back(checked_intercomm(idx));

    if (!selected_intercomms.empty())
    {
        std::set<std::string> noncollective_datasets;

        if (noncollective_dataset_writes)
            noncollective_datasets = noncollective_dataset_writes();

        Index index(local, selected_intercomms, &files, this, perform_indexing, noncollective_datasets);
        // we only serve if there any datasets; this matched old (pre-RPC)
        // behavior, but is probably not the right universal solution; we should
        // make this behavior user-configurable
        if (index.indexed_datasets || !perform_indexing)
            index.serve();

    }

    if (delete_data)
        clear_files();

    log->trace("serve_all done");
}

void
DistMetadataVOL::
broadcast_files(int root)
{
    diy::mpi::communicator local_(local);
    diy::MemoryBuffer bb;

    if (local_.rank() == root)
    {
        diy::save(bb, files.size());
        for (auto& x : files)
        {
            diy::save(bb, x.first);     // filename
            serialize(bb, x.second, *this);    // File*
        }
        diy::mpi::broadcast(local_, bb.buffer, root);
    } else
    {
        diy::mpi::broadcast(local_, bb.buffer, root);

        size_t n_files;
        diy::load(bb, n_files);
        std::map<std::string, std::unique_ptr<Object>> received;
        for (size_t i = 0; i < n_files; ++i)
        {
            std::string filename;
            diy::load(bb, filename);
            std::unique_ptr<Object> file(deserialize(bb, *this));
            if (!received.emplace(filename, std::move(file)).second)
                throw MetadataError("Duplicate filename in broadcast metadata");
        }
        // A broadcast replaces the authoritative snapshot. Existing wrappers
        // keep their old tree alive through drop() until their last close.
        clear_files();
        for (auto& entry : received)
        {
            if (!files.emplace(entry.first, entry.second.get()).second)
                throw MetadataError("Could not replace broadcast metadata");
            entry.second.release();
        }
    }
}

void
DistMetadataVOL::
make_remote_dataset(ObjectPointers*& result, std::pair<std::string, std::string> filepath)
{
    auto* original = static_cast<Object*>(result->mdata_obj);
    if (dynamic_cast<RemoteDataset*>(original))
        return;
    auto* dset = dynamic_cast<Dataset*>(original);
    log_assert(dset, "Object ({},{}) must be a dataset", filepath.first, original->name);

    auto log = get_logger();
    log->trace("Changing Dataset to RemoteDataset");

    auto* rf = dynamic_cast<RemoteFile*>(original->find_root());
    log_assert(rf, "Root ({},{}) must be a remote file", filepath.first,
               original->find_root()->name);

    auto ds_obj = rf->obj.call<rpc::client::object>("dataset_open", filepath.second);

    // Assume we are the consumer, since nothing is stored in memory.
    auto* ds = new RemoteDataset(original->name, std::move(ds_obj), std::move(filepath));
    ds->token = dset->token;
    ds->dcpl = dset->dcpl;
    ds->dapl = dset->dapl;
    ds->fill_data = std::move(dset->fill_data);
    ds->extra = dset->extra;
    dset->extra = nullptr;

    Object* parent = original->parent;
    if (parent)
    {
        auto it = std::find(parent->children.begin(), parent->children.end(), original);
        assert(it != parent->children.end());
        *it = ds;
    }
    else
    {
        auto it = std::find(rf->unlinked.begin(), rf->unlinked.end(), original);
        assert(it != rf->unlinked.end());
        *it = ds;
    }
    ds->parent = parent;
    ds->set_root(rf);
    original->parent = nullptr;
    ds->move_children(original);

    auto retarget_links = [&](Object* object, auto&& retarget_links_ref) -> void
    {
        if (auto* link = dynamic_cast<HardLink*>(object);
            link && link->target == original)
            link->target = ds;
        for (auto* child : object->children)
            retarget_links_ref(child, retarget_links_ref);
    };
    retarget_links(rf, retarget_links);
    for (auto* object : rf->unlinked)
        retarget_links(object, retarget_links);
    for (auto* pointer : our_objects)
    {
        auto* wrapped = static_cast<ObjectPointers*>(pointer);
        if (wrapped->mdata_obj == original)
            wrapped->mdata_obj = ds;
    }

    delete original;
}

void*
DistMetadataVOL::
object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("enter DistMetadataVOL::object_open");

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    std::pair<std::string, std::string> requested_path;
    bool has_requested_path = false;
    auto* location = obj_ ? static_cast<Object*>(obj_->mdata_obj) : nullptr;
    if (location && loc_params->type == H5VL_OBJECT_BY_NAME)
    {
        requested_path = location->fullname(loc_params->loc_data.loc_by_name.name);
        has_requested_path = true;
    }
    else if (location && loc_params->type == H5VL_OBJECT_BY_IDX)
    {
        auto link = location->locate_link(*loc_params);
        if (link.obj && link.path.empty())
        {
            requested_path = link.obj->fullname();
            has_requested_path = true;
        }
    }

    ObjectPointers* result = (ObjectPointers*) MetadataVOL::object_open(obj, loc_params, opened_type, dxpl_id, req);
    if (!result)
        return nullptr;

    Object* mdata_obj = (Object*) result->mdata_obj;
    if (auto* dataset = dynamic_cast<Dataset*>(mdata_obj);
        dataset && dataset->is_memory && RemoteObject::query(mdata_obj))
    {
        auto filepath = has_requested_path ? requested_path :
                                             mdata_obj->fullname();
        make_remote_dataset(result, filepath);
    }

    return (void*) result;
}

void*
DistMetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("enter DistMetadataVOL::dataset_open");

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::dataset_open(obj, loc_params, name, dapl_id, dxpl_id, req);
    if (!result)
        return nullptr;
    log->trace("Opening dataset (dist): {} {} {}", name, fmt::ptr(result->mdata_obj), fmt::ptr(result->h5_obj));

    // trace object back to root to build full path and file name
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    log->trace("Opening dataset: {} {} {}", name, filepath.first, filepath.second);

    Object* mdata_obj = (Object*) result->mdata_obj;
    if (auto* dataset = dynamic_cast<Dataset*>(mdata_obj);
        dataset && dataset->is_memory && RemoteObject::query(mdata_obj))
        make_remote_dataset(result, filepath);

    return (void*)result;
}

herr_t
DistMetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();
    log->trace("enter DistMetadataVOL::dataset_read");
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj);
    if (ds)
    {
        Hid file_extent(file_space_id == H5S_ALL ? H5Scopy(ds->space.id) : 0, true);
        if (file_space_id == H5S_ALL)
        {
            if (file_extent.id < 0 || H5Sselect_all(file_extent.id) < 0)
                return -1;
            file_space_id = file_extent.id;
        }
        if (mem_space_id == H5S_ALL)
            mem_space_id = file_space_id;

        const hssize_t count = H5Sget_select_npoints(file_space_id);
        if (count < 0 || H5Sget_select_npoints(mem_space_id) != count ||
            H5Sselect_valid(file_space_id) <= 0 || H5Sselect_valid(mem_space_id) <= 0)
            return -1;
        if (count == 0)
            return 0;
        if (!buf)
            return -1;

        const int rank = H5Sget_simple_extent_ndims(ds->space.id);
        if (rank < 0 || H5Sget_simple_extent_ndims(file_space_id) != rank)
            return -1;
        if (rank)
        {
            std::vector<hsize_t> dims(rank), lower(rank), upper(rank);
            if (H5Sget_simple_extent_dims(ds->space.id, dims.data(), nullptr) < 0 ||
                H5Sget_select_bounds(file_space_id, lower.data(), upper.data()) < 0)
                return -1;
            for (int i = 0; i < rank; ++i)
                if (upper[i] >= dims[i])
                    return -1;
        }

        CALI_MARK_BEGIN("dist_metadata_vol_dataset_read_query");
        ds->query(Datatype(mem_type_id), Dataspace(file_space_id), Dataspace(mem_space_id), buf);
        CALI_MARK_END("dist_metadata_vol_dataset_read_query");
    } else if (dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))
    {
        return MetadataVOL::dataset_read(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    } else if (unwrap(dset_) && buf)        // TODO: why are we checking buf?
    {
        return VOLBase::dataset_read(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    } else if (buf)
    {
        throw MetadataError(fmt::format("dataset_read(): neither memory, nor passthru open"));
    }

    return 0;
}

void*
DistMetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("DistMetadataVOL::file_open()");

    if (before_file_open)
    {
        std::string name_(name);
        before_file_open(name_);
    }

    std::string filename;
    if (set_filename) {
        filename = set_filename();
        name = filename.c_str();
        log->trace("file_open(): filename set to {} with set_filename callback", name);
    }

    bool need_memory = match_any(name, "", memory, true);
    bool need_passthru = match_any(name, "", passthru, true);

    if (not need_passthru and not need_memory)
        log->critical("DistMetadataVOL::file_open: neither memory nor passthru set for {}", name);

    if (has_real_file(name))
        return MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);

    if (need_passthru and not need_memory)
        return MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);

    // need_memory must be true.
    // First do the in-memory thing, so we force the producer to enter
    // serve_all to get file hierarchy. This ensures that the file was closed,
    // so when we call MetadataVOL::file_open, it will be on disk.
    // Will break, if so far the producer only created passthru datasets,
    // because indexed_datasets will be empty and we won't start serving.

    std::unique_ptr<Object> metadata;
    // get an intercomm for this file
    auto locs = find_matches(name, "", intercomm_locations, true);
    if (locs.empty())
        throw MetadataError(fmt::format("file_open(): no intercomm found for {}", name));

    std::set<int> dedup_indices;
    for(auto loc: locs)
        dedup_indices.emplace(intercomm_indices[loc]);

    if (dedup_indices.size() > 1)
        throw MetadataError(fmt::format("file_open(): found more than one intercomm for {}", name));

    int intercomm_index = *dedup_indices.begin();

    std::string name_ = name;
    std::unique_ptr<Query> query(new Query(local, intercomms, remote_size(intercomm_index), intercomm_index));
    diy::MemoryBuffer hierarchy = query->c.call<diy::MemoryBuffer>("get_file_hierarchy", name_);
    if (hierarchy.size() == 0 && hierarchy.nblobs() == 0)
    {
        MPI_Barrier(local);
        if (remote_file_counts_.find(intercomm_index) ==
            remote_file_counts_.end())
            query->send_done();
        return nullptr;
    }
    hierarchy.reset();
    metadata.reset(deserialize(hierarchy, *this));

    auto* hff = static_cast<File*>(metadata.get());
    if (hff->copy_whole) {
        MPI_Barrier(local);
        if (remote_file_counts_.find(intercomm_index) ==
            remote_file_counts_.end())
            query->send_done();
    } else {
        rpc::client::object rf = query->c.call<rpc::client::object>("file_open", std::string(name));

        log->trace("Creating remote");
        std::unique_ptr<RemoteFile> f(
            new RemoteFile(*this, *hff, std::move(rf), std::move(query)));

        if (H5Pget_driver(fapl_id) == H5FD_MPIO)
        {
            MPI_Info info = MPI_INFO_NULL;
            if (H5Pget_fapl_mpio(fapl_id, &f->comm, &info) < 0)
                throw MetadataError("Cannot retrieve remote file MPI driver properties");
            if (info != MPI_INFO_NULL)
                MPI_Info_free(&info);
        }

        // Move the hierarchy over and delete the plain File object.
        f->move_children(hff);
        f->unlinked = std::move(hff->unlinked);
        for (auto* object : f->unlinked)
            object->set_root(f.get());
        metadata.reset(f.release());
    }

    // Retire a dummy together with its map entry. Any wrappers already
    // referencing it retain that tree until their last close.
    drop(std::string(name));
    if (!files.emplace(name, metadata.get()).second)
        throw MetadataError("Could not replace remote file metadata");
    metadata.release();

    ObjectPointers* result = (ObjectPointers*)MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);
    if (!result)
    {
        drop(std::string(name));
        return nullptr;
    }

    return result;
}

herr_t
DistMetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    if (LowFive::get_log_level() <= spdlog::level::info)
        this->print_files();

    ++file_close_counter_;

    ObjectPointers* file_ = (ObjectPointers*) file;

    auto log = get_logger();
    log->trace("Enter DistMetadataVOL::file_close, mdata_obj = {}", fmt::ptr(file_->mdata_obj));
    if (file_->tmp) {
        log->trace("DistMetadataVOL::file_close, temporary reference, skipping close");
        return 0;
    }
    // this is a little too closely coupled to MetadataVOL::file_close(), but
    // it closes the file before starting to serve, which may be useful in some
    // scenarios
    if (unwrap(file_))
    {
        herr_t res = VOLBase::file_close(unwrap(file_), dxpl_id, req);
        if (res < 0) return res;
        file_->h5_obj = nullptr;    // this makes sure that the recursive call below won't trigger VOLBase::file_close() again
    }

    File* f = dynamic_cast<File*>((Object*) file_->mdata_obj);
    if (f && !dynamic_cast<RemoteFile*>(f))
    {
        log->trace("DistMetadataVOL::file_close, local file {}", f->name);
//         f->print();

        if (send_filename)
        {
            send_filename(f->name);
            log->trace("send_filename callback is done for file {}", f->name);
        }

        if (f->copy_of_remote)
        {
            log->trace("Local file is a copy of remote, skipping");
        } else if (serve_on_close && match_any(f->name, "", memory, true))      // TODO: is this the right place to serve? should we wait for all files to be closed?
        {
            log->trace("Closing file {}", fmt::ptr(f));
            serve_all();
        }
    }

    log->trace("DistMetadataVOL: calling file_close of base class {}", fmt::ptr(file));

    return MetadataVOL::file_close(file, dxpl_id, req);     // this is almost redundant; removes mdata_obj, if it's a File
}

DistMetadataVOL::FileNames
DistMetadataVOL::
get_filenames(int intercomm_index)
{
    auto log = get_logger();
    log->trace("DistMetadataVOL:get_filenames, intercomm_index={}", intercomm_index);
    Query query {local, intercomms, remote_size(intercomm_index), intercomm_index};      // NB: because no dataset is provided will only build index based on the intercomm
    return query.get_filenames();
}

void
DistMetadataVOL::
send_done(int intercomm_index)
{
    auto log = get_logger();
    log->trace("DistMetadataVOL:send_done, intercomm_index={}", intercomm_index);
    Query query {local, intercomms, remote_size(intercomm_index), intercomm_index};      // NB: because no dataset is provided will only build index based on the intercomm
    query.send_done();
}

void
DistMetadataVOL::
producer_signal_done()
{
    auto log = get_logger();
    log->trace("DistMetadataVOL:producer_signal_done");

    DataIntercomms indices(intercomms.size());

    if (serve_indices)
        indices = serve_indices();
    else
        std::iota(indices.begin(), indices.end(), 0);

    std::vector<MPI_Comm> selected_intercomms;
    for (auto idx : indices)
        selected_intercomms.push_back(checked_intercomm(idx));

    Files files;        // empty files
    Index index(local, selected_intercomms, &files, this);
    index.serve();
}

DistMetadataVOL&
DistMetadataVOL::create_DistMetadataVOL(communicator local_, communicator intercomm_)
{
    return create_DistMetadataVOL(local_, communicators {intercomm_});
}

DistMetadataVOL&
DistMetadataVOL::create_DistMetadataVOL(communicator local_, communicators intercomms_)
{
    auto log = get_logger();
    log->trace("Enter create_DistMetadataVOL");

    if (!info->vol)
    {
        log->trace("In create_DistMetadataVOL: info->vol is NULL, creating new DistMetadataVOL");
        info->vol = new DistMetadataVOL(local_, intercomms_);
    } else
    {
        log->warn("In create_DistMetadataVOL: info->vol is not NULL, return existing DistMetadataVOL, arguments ignored");
    }

    return *dynamic_cast<DistMetadataVOL*>(info->vol);
}

} // namespace LowFive
