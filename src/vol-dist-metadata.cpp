#include <lowfive/vol-dist-metadata.hpp>

void*
LowFive::DistMetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::dataset_open(obj, loc_params, name, dapl_id, dxpl_id, req);

    if (vol_properties.memory && !result->mdata_obj)
    {
        // assume we are the consumer, since nothing stored in memory (open also implies that)
        auto* ds = new RemoteDataset(name);     // build and record the index to be used in read

        // check that the dataset name is the full path (the only mode supported for now)
        // TODO: if only leaf name is given, could use backtrack_name to find full path
        // but that requires the user creating all the nodes (groups, etc.) in between the root and the leaf
        if (ds->name[0] != '/')
            throw MetadataError(fmt::format("Error: dataset_read(): Need full pathname for dataset {}", ds->name));

        // get the filename
        std::string filename, unused;
        backtrack_name(std::string(""), static_cast<Object*>(obj_->mdata_obj), filename, unused);

        // get the correct intercomm
        int intercomm_index;
        bool intercomm_found = false;
        for (auto& c : data_intercomms)
        {
            // o.filename and o.full_path can have wildcards '*' and '?'
            if (match(c.filename.c_str(), filename.c_str()) && match(c.full_path.c_str(), ds->name.c_str()))
            {
                intercomm_index = c.intercomm_index;
                intercomm_found = true;
                break;
            }
        }

        if (!intercomm_found)
            throw MetadataError(fmt::format("Error dataset_read(): no intercomm found for dataset {}", ds->name));

        // get the remote size of the intercomm
        int remote_size;
        int is_inter; MPI_Comm_test_inter(intercomms[intercomm_index], &is_inter);
        if (!is_inter)
            remote_size = intercomms[intercomm_index].size();
        else
            MPI_Comm_remote_size(intercomms[intercomm_index], &remote_size);

        // open a query
        Query* query = new Query(local, intercomms, remote_size, ds->name, intercomm_index);      // NB: because no dataset is provided will only build index based on the intercomm
        ds->query = query;
        result->mdata_obj = ds;
    }

    return (void*)result;
}

herr_t
LowFive::DistMetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    if (vol_properties.memory)
    {
        if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))                   // producer
            serve_data.push_back(ds);   // record dataset for serving later when file closes
        else if (RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj))  // consumer
        {
            Query* query = (Query*) ds->query;
            // NB: deferring calling query->close() until file_close() time
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

    if (vol_properties.memory)
    {
        // consumer with the name of a remote dataset
        if (RemoteDataset* ds = dynamic_cast<RemoteDataset*>((Object*) dset_->mdata_obj))
        {
            // query to producer
            Query* query = (Query*) ds->query;
            query->query(Dataspace(file_space_id), Dataspace(mem_space_id), buf);
        } else
        {
            // TODO: handle correctly
        }
    }

    // read from the file if not reading from memory
    // if both memory and passthru are enabled, read from memory only
    if (!vol_properties.memory && vol_properties.passthru && buf)     // TODO: probably also add a condition that read "from memory" failed
        return VOLBase::dataset_read(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::DistMetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(dset_->h5_obj), get_type, fmt::ptr(req));
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
    } else
    {
        // TODO: handle correctly
    }

    if (!vol_properties.memory && vol_properties.passthru)
        return VOLBase::dataset_get(dset_->h5_obj, get_type, dxpl_id, req, arguments);

    return 0;
}

herr_t
LowFive::DistMetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    File* f = dynamic_cast<File*>((Object*) file_->mdata_obj);

    if (vol_properties.memory)
    {
        // serve datasets (producer only)
        if (serve_data.size())              // only producer pushed any datasets to serve_data
        {
            Index index(local, intercomms, serve_data);
            index.serve();
        }
        else
        {
            // Calling Query::close() for each intercomm for this file in order to shut down the server on the producer.
            // Using data_intercomms to get filename and intercomm index, which can be duplicated for multiple datasets.
            // Only close the query once for a unique (filename, intercomm).
            using ClosedQuery = std::pair<std::string, int>;            // (filename, intercomm_index) of closed queries
            std::vector<ClosedQuery> closed_queries;
            for (auto i = 0; i < data_intercomms.size(); i++)
            {
                ClosedQuery closed_query = std::make_pair(f->name, data_intercomms[i].intercomm_index);
                if (f->name == data_intercomms[i].filename && std::find(closed_queries.begin(), closed_queries.end(), closed_query) == closed_queries.end())
                {
                    Query::close(local, intercomms[data_intercomms[i].intercomm_index]);
                    closed_queries.push_back(closed_query);
                }
            }
        }
    }

    return MetadataVOL::file_close(file, dxpl_id, req);
}
