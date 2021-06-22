#include <lowfive/vol-dist-metadata.hpp>

void*
LowFive::DistMetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* result = (ObjectPointers*) MetadataVOL::dataset_open(obj, loc_params, name, dapl_id, dxpl_id, req);

    if (vol_properties.memory && !result->mdata_obj)
    {
        // assume we are the consumer, since nothing stored in memory (open also implies that)
        // build and record the index to be used in read
        int remote_size;

        if (shared)
            remote_size = intercomms[0].size();
        else
            MPI_Comm_remote_size(intercomms[0], &remote_size);

        auto* ds = new RemoteDataset(name);

        // check that the dataset name is the full path (the only mode supported for now)
        // TODO: if only leaf name is given, could use backtrack_name to find full path
        // but that requires the user creating all the nodes (groups, etc.) in between the root and the leaf
        if (ds->name[0] != '/')
            throw MetadataError(fmt::format("Error: dataset_read(): Need full pathname for dataset {}", ds->name));

        Query* query = new Query(local, intercomms, remote_size, ds->name);      // NB: because no dataset is provided will only build index based on the intercomm

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
            // TODO: defer closing query until file close
//             query->close();
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
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

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
            // calling Query::close() in order to shut down the server on the producer
            // TODO: multiple different consumer tasks accessing the same server will be a problem
            // eventually need a different mechanism to shut down the server
            int remote_size;
            if (shared)
                remote_size = intercomms[0].size();
            else
                MPI_Comm_remote_size(intercomms[0], &remote_size);

            Query* query = new Query(local, intercomms, remote_size, std::string(""));
            query->close();
            delete query;
        }
    }

    return MetadataVOL::file_close(file, dxpl_id, req);
}
