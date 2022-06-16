#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/metadata/remote.hpp>
#include <lowfive/metadata/dummy.hpp>
#include "log-private.hpp"
#include "index.hpp"
#include "query.hpp"
#include <lowfive/metadata/serialization.hpp>


LowFive::DistMetadataVOL::DistMetadataVOL(communicator  local_, communicator  intercomm_):
    DistMetadataVOL(local_, communicators { std::move(intercomm_) })
{
    log->trace("DistMetadataVOL ctor: this = {}", fmt::ptr(this));
}


int
LowFive::DistMetadataVOL::
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
LowFive::DistMetadataVOL::
serve_all(bool delete_data)
{
    log->trace("enter serve_all, serve_data.size = {}", serve_data.size());

    // serve datasets (producer only)
    if (serve_data.size())              // only producer pushed any datasets to serve_data
    {
        Index index(local, intercomms, serve_data);
        index.serve();

        if (delete_data)
            for (auto* ds : serve_data)
                delete ds;
        serve_data.clear();
    }

    log->trace("serve_all done");
}

void*
LowFive::DistMetadataVOL::dataset_create(void* obj, const H5VL_loc_params_t* loc_params, const char* name,
        hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void** req)
{
    void* ds_ = MetadataVOL::dataset_create(obj, loc_params, name, lcpl_id,  type_id, space_id, dcpl_id, dapl_id,  dxpl_id, req);

    Object* ds_o = static_cast<Object*>(static_cast<ObjectPointers*>(ds_)->mdata_obj);
    if (Dataset* dset = dynamic_cast<Dataset*>(ds_o))
        serve_data.insert(dset);

    return ds_;
}

herr_t
LowFive::DistMetadataVOL::dataset_write(void* dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void* buf, void** req)
{
    Object* ds_o = static_cast<Object*>(static_cast<ObjectPointers*>(dset)->mdata_obj);
    if (Dataset* dataset = dynamic_cast<Dataset*>(ds_o))
        serve_data.insert(dataset);

    return MetadataVOL::dataset_write(dset, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

void*
LowFive::DistMetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
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
        if (DummyDataset* dd = dynamic_cast<DummyDataset*>((Object*) result->mdata_obj))
        {
            delete dd;

            // assume we are the consumer, since nothing stored in memory (open also implies that)
            auto* ds = new RemoteDataset(name);     // build and record the index to be used in read
            parent->add_child(ds);

            // check that the dataset name is the full path (the only mode supported for now)
            // TODO: if only leaf name is given, could use backtrack_name to find full path
            // but that requires the user creating all the nodes (groups, etc.) in between the root and the leaf
            if (ds->name[0] != '/')
                throw MetadataError(fmt::format("dataset_open(): Need full pathname for dataset {}", ds->name));

            // get the filename
            std::string filename = filepath.first;

            // TODO: might want to use filepath.second instead of ds->name

            // get the correct intercomm
            int loc = find_match(filename, ds->name, intercomm_locations);
            if (loc == -1)
                throw MetadataError(fmt::format("dataset_open(): no intercomm found for dataset {}", ds->name));
            int intercomm_index = intercomm_indices[loc];

            // open a query
            Query* query = new Query(local, intercomms, remote_size(intercomm_index), intercomm_index);      // NB: because no dataset is provided will only build index based on the intercomm
            query->dataset_open(ds->name);
            ds->query = query;
            result->mdata_obj = ds;
        }
    }

    return (void*)result;
}

herr_t
LowFive::DistMetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    log->trace("enter DistMetadataVOL::dataset_close");
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    if (dset_->mdata_obj)
    {
        if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))                   // producer
            serve_data.insert(ds);   // record dataset for serving later when file closes
        else if (RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj))  // consumer
        {
            Query* query = (Query*) ds->query;
            query->dataset_close();
            delete query;
        }
    }

    return MetadataVOL::dataset_close(dset, dxpl_id, req);
}

herr_t
LowFive::DistMetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    log->trace("enter DistMetadataVOL::dataset_read");
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj);
    if (ds)
    {
        //  TODO: support H5S_ALL, need to get file_space_id, mem_space_id with H5Dget_space
        // if (file_space_id == H5S_ALL)
        //     file_space_id = H5Dget_space(?);
        // consumer with the name of a remote dataset: query to producer
        Query* query = (Query*) ds->query;
        query->query(Dataspace(file_space_id), Dataspace(mem_space_id), buf);
    } else if (unwrap(dset_) && buf)        // TODO: why are we checking buf?
    {
        return VOLBase::dataset_read(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    } else if (buf)
    {
        throw MetadataError(fmt::format("dataset_read(): neither memory, nor passthru open"));
    }

    return 0;
}

herr_t
LowFive::DistMetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    log->trace("DistMetadataVOL::dataset_get dset = {}, get_type = {}, req = {}, dset_ = {}, dset_->mdata_ovbj = {}", fmt::ptr(unwrap(dset_)), get_type, fmt::ptr(req), fmt::ptr(dset_), fmt::ptr((Object*)dset_->mdata_obj));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    // consumer with the name of a remote dataset
    if (RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj))
    {
        // query to producer
        Query* query = (Query*) ds->query;

        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = query->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = query->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in dataset_get()", get_type));
        }
    } else if (unwrap(dset_))
    {
        return VOLBase::dataset_get(unwrap(dset_), get_type, dxpl_id, req, arguments);
    } else if (dset_->mdata_obj)
    {
        return MetadataVOL::dataset_get(dset_, get_type, dxpl_id, req, arguments);
    } else {
        throw MetadataError(fmt::format("In dataset_get(): neither memory, nor passthru open"));
    }

    return 0;
}

void*
LowFive::DistMetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    log->trace("DistMetadataVOL::file_open()");
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);

    // if producer opens existing file
    if (dynamic_cast<File*>((Object*) result->mdata_obj))
        return result;

    if (match_any(name, "", memory, true))
    {

        // if rank is producer, but file was created on another rank (as AMReX does: header is written by one rank per node, and data is written by all ranks on the node)
        // it can be not present on some producer ranks. We create file if necessary.
        // Assume: consumers must open file in read-only mode, otherwise it is producer
        // TODO: find a better solution, this might fail in many cases
        if (flags != H5F_ACC_RDONLY) {
            log->trace("DistMetadataVOL::file_open(), name = {}, local file not found, but opened not in RDONLY - create empty file", name);
            return MetadataVOL::file_create(name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id, dxpl_id, req);
        }

        // if there was DummyFile, delete it (if dynamic cast fails, delete nullptr is fine)
        delete dynamic_cast<DummyFile*>((Object*) result->mdata_obj);

        log->trace("Creating remote");
        RemoteFile* f = new RemoteFile(name);
        files.emplace(name, f);
        result->mdata_obj = f;

        // get an intercomm for this file
        auto locs = find_matches(name, "", intercomm_locations, true);
        if (locs.empty())
            throw MetadataError(fmt::format("file_open(): no intercomm found for {}", name));
        // signal to every intercomm that we are opening the file;
        // technically ought to dedup the intercomms, but signalling open once
        // per pattern works too, as long as it matches file_close()
        for (auto loc : locs)
        {
            int intercomm_index = intercomm_indices[loc];
            Query(local, intercomms, remote_size(intercomm_index), intercomm_index).file_open();
        }

    }

    return result;
}

herr_t
LowFive::DistMetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

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
        file_->h5_obj = nullptr;    // this makes sure that the recursive call below won't trigger VOLBase::file_close() again
    }

    if (RemoteFile* f = dynamic_cast<RemoteFile*>((Object*) file_->mdata_obj))
    {
        log->trace("DistMetadataVOL::file_close, remote file {}", f->name);
        // signal that we are done
        // get an intercomm for this file
        auto locs = find_matches(f->name, "", intercomm_locations, true);
        if (locs.empty())
            throw MetadataError(fmt::format("file_close(): no intercomm found for {}", f->name));
        // signal to every intercomm that we are opening the file
        for (auto loc : locs)
        {
            int intercomm_index = intercomm_indices[loc];
            Query(local, intercomms, remote_size(intercomm_index), intercomm_index).file_close();
        }

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

void*
LowFive::DistMetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::group_open(obj, loc_params, name, gapl_id, dxpl_id, req);

    // TODO: should we double-check that we are in a remote file?
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    if (match_any(filepath, memory, true))
        if (DummyGroup* dg = dynamic_cast<DummyGroup*>((Object*) result->mdata_obj)) // didn't find local
        {
            delete dg;
            result->mdata_obj = parent->add_child(new RemoteGroup(name));      // just store the name for future reference
        }

    return result;
}

LowFive::DistMetadataVOL::FileNames
LowFive::DistMetadataVOL::
get_filenames(int intercomm_index)
{
    log->trace("DistMetadataVOL:get_filenames, intercomm_index={}", intercomm_index);
    Query query {local, intercomms, remote_size(intercomm_index), intercomm_index};      // NB: because no dataset is provided will only build index based on the intercomm
    return query.get_filenames();
}

void
LowFive::DistMetadataVOL::
send_done(int intercomm_index)
{
    log->trace("DistMetadataVOL:send_done, intercomm_index={}", intercomm_index);
    Query query {local, intercomms, remote_size(intercomm_index), intercomm_index};      // NB: because no dataset is provided will only build index based on the intercomm
    query.send_done();
}

void
LowFive::DistMetadataVOL::
producer_signal_done()
{
    log->trace("DistMetadataVOL:producer_signal_done");
    Index index(local, intercomms, ServeData());
    index.serve();
}