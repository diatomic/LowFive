#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"
#include <cassert>

void*
LowFive::MetadataVOL::
dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
               const char *name,
               hid_t lcpl_id,  hid_t type_id,
               hid_t space_id, hid_t dcpl_id,
               hid_t dapl_id,  hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    auto log = get_logger();
    log->trace("Dataset Create");
    log->trace("dataset_create: parent obj = {}", *obj_);
    if (name)
        log->trace(", name = {}", name);
    else
        log->trace(", name = NULL");
    log->trace("");

    assert(obj_->mdata_obj);
    // trace object back to root to build full path and file name

    std::string name_str = name ? name : "";

    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name_str);

    if (unwrap(obj_) && match_any(filepath, passthru))
        result = wrap(VOLBase::dataset_create(unwrap(obj_), loc_params, name, lcpl_id,  type_id, space_id, dcpl_id, dapl_id,  dxpl_id, req));
    else
        result = wrap(nullptr);

    // check the ownership map for the full path name and file name
    Dataset::Ownership own = Dataset::Ownership::lowfive;
    if (match_any(filepath, zerocopy))
        own = Dataset::Ownership::user;

    // add the dataset
    auto obj_path = static_cast<Object*>(obj_->mdata_obj)->search(name_str);
    assert(obj_path.is_name());
    result->mdata_obj = obj_path.obj->add_child(new Dataset(obj_path.path, type_id, space_id, own, dcpl_id));

    log->trace("created dataset in metadata, new object {} under parent object {} named {}",
            *result, obj_->mdata_obj, static_cast<Object*>(obj_->mdata_obj)->name);

    return (void*)result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    auto log = get_logger();
    log->trace("MetadataVOL::dataset_open {}", *obj_);

    // trace object back to root to build full path and file name
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    if (match_any(filepath, passthru))
        result = wrap(VOLBase::dataset_open(unwrap(obj_), loc_params, name, dapl_id, dxpl_id, req));
    else
        result = wrap(nullptr);

    if (match_any(filepath, memory))
    {
        // find the dataset in our file metadata
        auto obj_path = parent->search(name);
        if (obj_path.path.empty())
            result->mdata_obj = obj_path.obj;
    }

    if (!result->mdata_obj)
    {
        // TODO: create group structure on the fly
        result->mdata_obj = parent->add_child(new DummyDataset(name));
    }

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    auto log = get_logger();
    log->trace("dset = {}, get_type = {}, req = {}", *dset_, get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    // TODO: Why do we prefer passthru to memory here? Only reason is that it's
    //       more complete. But perhaps it's best to trigger the error in the else
    //       than survive on passthru?
    herr_t result = 0;
    if (unwrap(dset_))
        result = VOLBase::dataset_get(unwrap(dset_), get_type, dxpl_id, req, arguments);
    else if (dset_->mdata_obj)
    {                                                       // see hdf5 H5VLnative_dataset.c, H5VL__native_dataset_get()
        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = static_cast<Dataset*>(dset_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = static_cast<Dataset*>(dset_->mdata_obj)->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_DCPL)
        {
            log->trace("GET_DCPL");
            hid_t *ret = va_arg(args, hid_t*);
            *ret = static_cast<Dataset*>(dset_->mdata_obj)->dcpl.id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);

            // DEPRECATE
//             // create a new empty property list;
//             *ret = H5Pcreate(H5P_DATASET_CREATE);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in dataset_get()", get_type));
        }
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto log = get_logger();
    log->trace("dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}",
               *dset_,
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (unwrap(dset_))
        return VOLBase::dataset_read(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    else if (dset_->mdata_obj)
    {
        Dataset* ds = (Dataset*) dset_->mdata_obj;              // dataset from our metadata

        // sanity check that the datatype and dimensionality being read matches the metadata
        // TODO: HDF5 allows datatypes to not match and takes care of the conversion;
        //       eventually, we need to support this functionality as well
        if (ds->type.dtype_class != convert_type_class(H5Tget_class(mem_type_id)) ||
                ds->type.dtype_size != H5Tget_size(mem_type_id))
            throw MetadataError(fmt::format("Error: dataset_read(): type mismatch"));
        if (file_space_id != H5S_ALL && Dataspace(file_space_id).dim != ds->space.dim)
            throw MetadataError(fmt::format("Error: dataset_read(): dim mismatch"));

        for (auto& dt : ds->data)                               // for all the data triples in the metadata dataset
        {
            Dataspace& fs = dt.file;

            hid_t file_space_id_1 = (file_space_id == H5S_ALL) ? fs.id : file_space_id;
            hid_t mem_space_id_1  = (mem_space_id == H5S_ALL)  ? fs.id : mem_space_id;
            hid_t mem_dst = Dataspace::project_intersection(file_space_id_1, mem_space_id_1, fs.id);
            hid_t mem_src = Dataspace::project_intersection(fs.id,           dt.memory.id,   file_space_id_1);

            Dataspace dst(mem_dst, true);
            Dataspace src(mem_src, true);

            Dataspace::iterate(dst, Datatype(mem_type_id).dtype_size, src, ds->type.dtype_size, [&](size_t loc1, size_t loc2, size_t len)
                    {
                    std::memcpy((char*) buf + loc1, (char*) dt.data + loc2, len);
                    });

            log->trace("dst = {}", dst);
            log->trace("src = {}", src);
        }   // for all data triples
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto log = get_logger();
    log->trace("MetadataVOL::dataset_write, dset = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})",
               *dset_,
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (dset_->mdata_obj)
    {
        // save our metadata
        Dataset* ds = (Dataset*) dset_->mdata_obj;
        ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);
    }

    if (unwrap(dset_))
        return VOLBase::dataset_write(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_specific(void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("dataset_specific: dset = {} specific_type = {}", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::dataset_specific(unwrap(obj_), specific_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        va_list args;
        va_copy(args,arguments);

        // specific types are enumerated in H5VLconnector.h
        switch (specific_type)
        {
            case H5VL_DATASET_SET_EXTENT:
            {
                // adapted from H5VLnative_dataset.c, H5VL__native_dataset_specific()
                const hsize_t *size = va_arg(args, const hsize_t*);
                Dataspace& space = static_cast<Dataset*>(obj_->mdata_obj)->space;
                space.set_extent(size); // NB: updating overall dataspace, not individual data triples
                break;
            }
            case H5VL_DATASET_FLUSH:
            {
                log->trace("dataset_specific specific_type H5VL_DATASET_FLUSH is a no-op in metadata");
                break;
            }
            case H5VL_DATASET_REFRESH:
            {
                log->trace("dataset_specific specific_type H5VL_DATASET_REFRESH is a no-op in metadata");
                break;
            }
        }
    }
    else
        throw MetadataError(fmt::format("dataset_specific(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
dataset_optional(void *obj, H5VL_dataset_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("dataset_optional: dset = {} optional_type = {}", *obj_, opt_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::dataset_optional(unwrap(obj_), opt_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_DATASET_* constants)
        log->warn("Warning: dataset_optional not implemented in metadata yet");
    }
    else
        throw MetadataError(fmt::format("dataset_optional(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto log = get_logger();
    log->trace("enter MetadataVOL::dataset_close");
    if (dset_->tmp)
    {
        log->trace("temporary reference, skipping close");
        return 0;
    }
    log->trace("dataset_close: dset = {}, dxpl_id = {}", *dset_, dxpl_id);

    herr_t retval = 0;

    // close the file, keep in-memory structures in the hierarchy (they may be needed up to file_close())
    if (unwrap(dset_))
        retval = VOLBase::dataset_close(unwrap(dset_), dxpl_id, req);

    // TODO: not sure what the point of this was; the comment above offers a clue, but I still don't understand the logic
    //else if (vol_properties.passthru && vol_properties.memory)
    //{
    //    if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))
    //        retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    //}

    return retval;
}
