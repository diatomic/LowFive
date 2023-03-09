#include <lowfive/vol-dist-metadata.hpp>
#include "metadata/remote.hpp"
#include "metadata/dummy.hpp"
#include "log-private.hpp"
#include "index.hpp"
#include "query.hpp"
#include "metadata/serialization.hpp"

#include "vol-metadata-private.hpp"     // ObjectPointers

namespace LowFive {

DistMetadataVOL::DistMetadataVOL(communicator  local_, communicator  intercomm_):
    DistMetadataVOL(local_, communicators { std::move(intercomm_) })
{
    auto log = get_logger();
    log->trace("DistMetadataVOL ctor: this = {}", fmt::ptr(this));
}


int
DistMetadataVOL::
remote_size(int intercomm_index)
{
    // get the remote size of the intercomm: different logic for inter- and intra-comms
    int remote_size;
    int is_inter; MPI_Comm_test_inter(intercomms[intercomm_index], &is_inter);
    if (!is_inter)
        MPI_Comm_size(intercomms[intercomm_index], &remote_size);
    else
        MPI_Comm_remote_size(intercomms[intercomm_index], &remote_size);

    return remote_size;
}

void
DistMetadataVOL::
serve_all(bool delete_data)
{
    auto log = get_logger();
    log->trace("enter serve_all, files.size = {}", files.size());

    DataIntercomms indices(intercomms.size());

    if (serve_indices)
        indices = serve_indices();
    else
        std::iota(indices.begin(), indices.end(), 0);

    std::vector<MPI_Comm> selected_intercomms;
    for (auto idx : indices)
        selected_intercomms.push_back(intercomms[idx]);

    if (!selected_intercomms.empty())
    {
        Index index(local, selected_intercomms, &files);
        // we only serve if there any datasets; this matched old (pre-RPC)
        // behavior, but is probably not the right universal solution; we should
        // make this behavior user-configurable
        if (index.indexed_datasets)
            index.serve();

    }

    // TODO: what do we do about delete_data? Probably should become "clear files"
    //if (delete_data)
    //    for (auto* ds : serve_data)
    //        delete ds;

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
            serialize(bb, x.second);    // File*
        }
        diy::mpi::broadcast(local_, bb.buffer, root);
    } else
    {
        diy::mpi::broadcast(local_, bb.buffer, root);

        size_t n_files;
        diy::load(bb, n_files);
        // NB: we are not clearing files beforehand, might be a reasonable thing to do
        for (size_t i = 0; i < n_files; ++i)
        {
            std::string filename;
            diy::load(bb, filename);
            auto* f = deserialize(bb);
            files.emplace(filename, f);
        }
    }
}

void
DistMetadataVOL::
make_remote_dataset(ObjectPointers*& result, std::pair<std::string, std::string> filepath)
{
    Object* mdata_obj = (Object*) result->mdata_obj;
    Dataset* dset = dynamic_cast<Dataset*>(mdata_obj);

    auto log = get_logger();
    log->trace("Changing Dataset to RemoteDataset");

    Object* f = mdata_obj->find_root();
    RemoteFile* rf = dynamic_cast<RemoteFile*>(f);
    log_assert(rf, "Root ({},{}) must be a remote file", filepath.first, f->name);

    auto ds_obj = rf->obj.call<rpc::client::object>("dataset_open", filepath.second);

    // assume we are the consumer, since nothing stored in memory (open also implies that)
    auto* ds = new RemoteDataset(mdata_obj->name, std::move(ds_obj));     // build and record the index to be used in read
    ds->dcpl = dset->dcpl;
    mdata_obj->parent->add_child(ds);
    ds->move_children(mdata_obj);
    result->mdata_obj = ds;
    delete mdata_obj;
}

void*
DistMetadataVOL::
object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("enter DistMetadataVOL::object_open");

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::object_open(obj, loc_params, opened_type, dxpl_id, req);

    Object* mdata_obj = (Object*) result->mdata_obj;
    if (mdata_obj)
    {
        auto filepath = mdata_obj->fullname();
        if (match_any(filepath,memory))
        {
            if (dynamic_cast<Dataset*>(mdata_obj) && RemoteObject::query(mdata_obj))
                make_remote_dataset(result, filepath);
        }
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
    log->trace("Opening dataset (dist): {} {} {}", name, fmt::ptr(result->mdata_obj), fmt::ptr(result->h5_obj));

    // trace object back to root to build full path and file name
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    log->trace("Opening dataset: {} {} {}", name, filepath.first, filepath.second);

    if (match_any(filepath,memory))
    {
        Object* mdata_obj = (Object*) result->mdata_obj;

        log->trace("Checking if Dataset: {}", fmt::ptr(mdata_obj));

        // Dataset that really should be RemoteDataset
        if (dynamic_cast<Dataset*>(mdata_obj) && RemoteObject::query(mdata_obj))
            make_remote_dataset(result, filepath);

        log_assert(dynamic_cast<RemoteDataset*>((Object*) result->mdata_obj), "Object must be a RemoteDataset");
    }

    return (void*)result;
}

herr_t
DistMetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    auto log = get_logger();
    log->trace("enter DistMetadataVOL::dataset_read");
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj);
    if (ds)
    {
        //  TODO: support H5S_ALL, need to get file_space_id, mem_space_id with H5Dget_space
        // if (file_space_id == H5S_ALL)
        //     file_space_id = H5Dget_space(?);
        // consumer with the name of a remote dataset: query to producer
        ds->query(Dataspace(file_space_id), Dataspace(mem_space_id), buf);
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
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);

    // if producer opens existing file
    if (dynamic_cast<File*>((Object*) result->mdata_obj))
        return result;

    if (match_any(name, "", memory, true))
    {
        // get an intercomm for this file
        auto locs = find_matches(name, "", intercomm_locations, true);
        if (locs.empty())
            throw MetadataError(fmt::format("file_open(): no intercomm found for {}", name));

        std::set<int> dedup_indices;
        for (auto loc : locs)
            dedup_indices.emplace(intercomm_indices[loc]);

        if (dedup_indices.size() > 1)
            throw MetadataError(fmt::format("file_open(): found more than one intercomm for {}", name));

        int intercomm_index = *dedup_indices.begin();

        std::string name_ = name;
        std::unique_ptr<Query> query(new Query(local, intercomms, remote_size(intercomm_index), intercomm_index));
        diy::MemoryBuffer hierarchy = query->c.call<diy::MemoryBuffer>("get_file_hierarchy", name_);
        hierarchy.reset();
        Object* hf = deserialize(hierarchy);

        rpc::client::object rf = query->c.call<rpc::client::object>("file_open", std::string(name));

        // if there was DummyFile, delete it (if dynamic cast fails, delete nullptr is fine)
        delete dynamic_cast<DummyFile*>((Object*) result->mdata_obj);
        log->trace("Creating remote");
        RemoteFile* f = new RemoteFile(name, std::move(rf), std::move(query));
        files.emplace(name, f);
        result->mdata_obj = f;

        // move the hierarchy over and delete the plain File object hf
        f->move_children(hf);
        delete hf;
    }

    return result;
}

herr_t
DistMetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
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
        if (res < 0)
            log->error("Error when closing file");
        file_->h5_obj = nullptr;    // this makes sure that the recursive call below won't trigger VOLBase::file_close() again
    }

    if (RemoteFile* f = dynamic_cast<RemoteFile*>((Object*) file_->mdata_obj))
    {
        log->trace("DistMetadataVOL::file_close, remote file {}", f->name);
        f->obj.call<void>("file_close");

        MPI_Barrier(local);
        int rank; MPI_Comm_rank(local, &rank);
        bool root = rank == 0;
        if (root)
            f->obj.self_->finish(0);

        files.erase(f->name);
        log->trace("DistMetadataVOL::file_close delete {}", fmt::ptr(f));
        delete f;       // erases all the children too
        file_->mdata_obj = nullptr;
    } else if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        log->trace("DistMetadataVOL::file_close, local file {}", f->name);
        if (serve_on_close && match_any(f->name, "", memory, true))      // TODO: is this the right place to serve? should we wait for all files to be closed?
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

    Files files;        // empty files
    Index index(local, intercomms, &files);
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
