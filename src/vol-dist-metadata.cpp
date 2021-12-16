#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/metadata/remote.hpp>
#include <lowfive/metadata/dummy.hpp>



int
LowFive::DistMetadataVOL::
remote_size(int intercomm_index)
{
    // get the remote size of the intercomm: different logic for inter- and intra-comms
    int remote_size;
    int is_inter; MPI_Comm_test_inter(intercomms[intercomm_index], &is_inter);
    if (!is_inter)
        remote_size = intercomms[intercomm_index].size();
    else
        MPI_Comm_remote_size(intercomms[intercomm_index], &remote_size);

    return remote_size;
}

void*
LowFive::DistMetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::dataset_open(obj, loc_params, name, dapl_id, dxpl_id, req);
    fmt::print(stderr, "Opening dataset (dist): {} {} {}\n", name, fmt::ptr(result->mdata_obj), fmt::ptr(result->h5_obj));

    // trace object back to root to build full path and file name
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    fmt::print(stderr, "Opening dataset: {} {} {}\n", name, filepath.first, filepath.second);

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
                throw MetadataError(fmt::format("Error: dataset_open(): Need full pathname for dataset {}", ds->name));

            // get the filename
            std::string filename = filepath.first;

            // TODO: might want to use filepath.second instead of ds->name

            // get the correct intercomm
            int loc = find_match(filename, ds->name, intercomm_locations);
            if (loc == -1)
                throw MetadataError(fmt::format("Error dataset_open(): no intercomm found for dataset {}", ds->name));
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
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    if (dset_->mdata_obj)
    {
        if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))                   // producer
            serve_data.push_back(ds);   // record dataset for serving later when file closes
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
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj);
    if (ds)
    {
        // consumer with the name of a remote dataset: query to producer
        Query* query = (Query*) ds->query;
        query->query(Dataspace(file_space_id), Dataspace(mem_space_id), buf);
    } else if (unwrap(dset_) && buf)        // TODO: why are we checking buf?
    {
        return VOLBase::dataset_read(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    } else if (buf)
    {
        throw MetadataError(fmt::format("Error: dataset_read(): neither memory, nor passthru open"));
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

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(unwrap(dset_)), get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    // consumer with the name of a remote dataset
    if (RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj))
    {
        // query to producer
        Query* query = (Query*) ds->query;

        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = query->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = query->type;

            fmt::print("dataset data type id = {}, datatype = {}\n",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            fmt::print("copied data type id = {}, datatype = {}\n",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Warning, unknown get_type == {} in dataset_get()", get_type));
        }
    } else if (unwrap(dset_))
    {
        return VOLBase::dataset_get(unwrap(dset_), get_type, dxpl_id, req, arguments);
    } else {
        throw MetadataError(fmt::format("In dataset_get(): neither memory, nor passthru open"));
    }

    return 0;
}

void*
LowFive::DistMetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    fmt::print(stderr, "DistMetadataVOL::file_open()\n");
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::file_open(name, flags, fapl_id, dxpl_id, req);

    if (match_any(name, "", memory, true))
    {
        DummyFile* df = dynamic_cast<DummyFile*>((Object*) result->mdata_obj);
        assert(df);
        delete df;

        fmt::print("Creating remote\n");
        RemoteFile* f = new RemoteFile(name);
        files.emplace(name, f);
        result->mdata_obj = f;

        // get an intercomm for this file
        auto locs = find_matches(name, "", intercomm_locations, true);
        if (locs.empty())
            throw MetadataError(fmt::format("Error file_open(): no intercomm found for {}", name));
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
        // signal that we are done
        // get an intercomm for this file
        auto locs = find_matches(f->name, "", intercomm_locations, true);
        if (locs.empty())
            throw MetadataError(fmt::format("Error file_close(): no intercomm found for {}", f->name));
        // signal to every intercomm that we are opening the file
        for (auto loc : locs)
        {
            int intercomm_index = intercomm_indices[loc];
            Query(local, intercomms, remote_size(intercomm_index), intercomm_index).file_close();
        }

        files.erase(f->name);
        delete f;       // erases all the children too
        file_->mdata_obj = nullptr;
    } else if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        if (match_any(f->name, "", memory, true))      // TODO: is this the right place to serve? should we wait for all files to be closed?
        {
            fmt::print("Closing file {}\n", fmt::ptr(f));

            // serve datasets (producer only)
            if (serve_data.size())              // only producer pushed any datasets to serve_data
            {
                Index index(local, intercomms, serve_data);
                index.serve();
            }
        }
    }

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
