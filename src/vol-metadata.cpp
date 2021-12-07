#include <lowfive/vol-metadata.hpp>
#include <cassert>

// General design philosophy:
//  - build in-memory hierarchy, regardless of whether it was requested, but if
//    it wasn't, don't store the actual data
//  - if passthru is enabled for the particular item, prefer it (as an
//    iron-clad way to get what the user wants);
//    TODO: this may be a wrong decision: the more logical way would be to always
//    prefer memory, since it's faster, but passthru is safer; it's worth re-visiting
//  - of course, it's perfectly reasonable to have both memory and passthru on
//    producer, and only memory on consumer

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ptrs = nullptr;

    // create our file metadata; NB: we build the in-memory hierarchy regardless of whether we match memory
    void* mdata = nullptr;
    std::string name_(name);
    File* f = new File(name_);
    files.emplace(name_, f);
    mdata = f;

    if (match_any(name, "", passthru, true))
        obj_ptrs = (ObjectPointers*) VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    fmt::print(stderr, "file_optional: opt_type = {}\n", opt_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    herr_t res = 0;
    if (unwrap(file_))
        res = VOLBase::file_optional(file_, opt_type, dxpl_id, req, arguments);

    return res;
}

void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    fmt::print("DistMetadataVOL::file_open()\n");
    ObjectPointers* obj_ptrs = nullptr;

    // find the file in the VOL
    void* mdata = nullptr;
    std::string name_(name);
    auto it = files.find(name_);
    if (it != files.end())
        mdata = it->second;

    if (match_any(name, "", passthru, true))
        obj_ptrs = (ObjectPointers*) VOLBase::file_open(name, flags, fapl_id, dxpl_id, req);
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    fmt::print("Opened file {}: {} {}\n", name, fmt::ptr(obj_ptrs->mdata_obj), fmt::ptr(obj_ptrs->h5_obj));

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    va_list args;
    va_copy(args,arguments);

    fmt::print("file = {}, get_type = {}, req = {}\n", fmt::ptr(file_->h5_obj), get_type, fmt::ptr(req));
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(file_))
        result = VOLBase::file_get(file_, get_type, dxpl_id, req, arguments);
    // else: TODO
    else
        fmt::print("Warning: requested file_get() not implemented for in-memory regime\n");

    return result;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    herr_t res = 0;

    if (unwrap(file_))
        res = VOLBase::file_close(file_, dxpl_id, req);

    if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        // we created this file
        files.erase(f->name);
        delete f;       // erases all the children too
    }

    // deliberately verbose, to emphasize checking of res
    if (res == 0)
        return 0;
    else
        return res;
}


void*
LowFive::MetadataVOL::
dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
               const char *name,
               hid_t lcpl_id,  hid_t type_id,
               hid_t space_id, hid_t dcpl_id,
               hid_t dapl_id,  hid_t dxpl_id, void **req)
{
    //fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
    //fmt::print("data type = {}\n", Datatype(type_id));
    //fmt::print("data space = {}\n", Dataspace(space_id));

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print("create: dset = {}, dxpl_id = {}\n", fmt::ptr(obj_->h5_obj), dxpl_id);

    assert(obj_->mdata_obj);
    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    if (unwrap(obj_) && match_any(filepath, passthru))
        result = (ObjectPointers*) VOLBase::dataset_create(obj_, loc_params, name, lcpl_id,  type_id, space_id, dcpl_id, dapl_id,  dxpl_id, req);
    else
        result = new ObjectPointers;

    // check the ownership map for the full path name and file name
    Dataset::Ownership own = Dataset::Ownership::lowfive;
    if (match_any(filepath, zerocopy))
        own = Dataset::Ownership::user;

    // add the dataset
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name, type_id, space_id, own));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print("dataset_open {} {}\n", fmt::ptr(obj_->mdata_obj), fmt::ptr(obj_->h5_obj));

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    if (match_any(filepath, passthru))
        result = (ObjectPointers*) VOLBase::dataset_open(obj_, loc_params, name, dapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    if (match_any(filepath, memory))
        // find the dataset in our file metadata
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(unwrap(dset_)), get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    // TODO: Why do we prefer passthru to memory here? Only reason is that it's
    //       more complete. But perhaps it's best to trigger the error in the else
    //       than survive on passthru?
    herr_t result = 0;
    if (unwrap(dset_))
        result = VOLBase::dataset_get(dset_, get_type, dxpl_id, req, arguments);
    else if (dset_->mdata_obj)
    {
        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Dataset*>(dset_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Dataset*>(dset_->mdata_obj)->type;

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
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}\n",
               fmt::ptr(unwrap(dset_)),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (unwrap(dset_))
        return VOLBase::dataset_read(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    else if (dset_->mdata_obj)
    {
        Dataset* ds = (Dataset*) dset_->mdata_obj;              // dataset from our metadata

        // sanity check that the datatype and dimensionality being read matches the metadata
        // TODO: HDF5 allows datatypes to not match and takes care of the conversion;
        //       eventually, we need to support this functionality as well
        if (ds->type.dtype_class != convert_type_class(H5Tget_class(mem_type_id)) ||
                ds->type.dtype_size != H5Tget_size(mem_type_id))
            throw MetadataError(fmt::format("Error: dataset_read(): type mismatch"));
        if (Dataspace(file_space_id).dim != ds->space.dim)
            throw MetadataError(fmt::format("Error: dataset_read(): dim mismatch"));

        for (auto& dt : ds->data)                               // for all the data triples in the metadata dataset
        {
            Dataspace& fs = dt.file;

            hid_t mem_dst = Dataspace::project_intersection(file_space_id, mem_space_id, fs.id);
            hid_t mem_src = Dataspace::project_intersection(fs.id,         dt.memory.id, file_space_id);

            Dataspace dst(mem_dst, true);
            Dataspace src(mem_src, true);

            Dataspace::iterate(dst, Datatype(mem_type_id).dtype_size, src, ds->type.dtype_size, [&](size_t loc1, size_t loc2, size_t len)
                    {
                    std::memcpy((char*) buf + loc1, (char*) dt.data + loc2, len);
                    });

            fmt::print("dst = {}\n", dst);
            fmt::print("src = {}\n", src);
        }   // for all data triples
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}, dset->mdata_obj = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})\n",
               fmt::ptr(unwrap(dset_)),
               fmt::ptr(dset_->mdata_obj),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (dset_->mdata_obj)
    {
        // save our metadata
        Dataset* ds = (Dataset*) dset_->mdata_obj;
        ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);
    }

    if (unwrap(dset_))
        return VOLBase::dataset_write(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    fmt::print("close: dset = {}, dxpl_id = {}\n", fmt::ptr(unwrap(dset_)), dxpl_id);

    herr_t retval = 0;

    // close the file, keep in-memory structures in the hierarchy (they may be needed up to file_close())
    if (unwrap(dset_))
        retval = VOLBase::dataset_close(dset_, dxpl_id, req);

    // TODO: not sure what the point of this was; the comment above offers a clue, but I still don't understand the logic
    //else if (vol_properties.passthru && vol_properties.memory)
    //{
    //    if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))
    //        retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    //}

    return retval;
}

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}, mdata = {}\n", loc_params->type, name, obj_->mdata_obj != nullptr);

    assert(obj_->mdata_obj);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = (ObjectPointers*) VOLBase::group_create(obj_, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // add group to our metadata
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Group(name));

    return result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = (ObjectPointers*) VOLBase::group_open(obj_, loc_params, name, gapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the group in our file metadata
    std::string name_(name);
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = 0;

    if (unwrap(grp_))
        retval = VOLBase::group_close(grp_, dxpl_id, req);

    return retval;
}

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Attr Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath,passthru))
        result = (ObjectPointers*) VOLBase::attr_create(obj_, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // add attribute to our metadata; NB: attribute cannot have children, so only creating it if we have to
    if (match_any(filepath,memory))
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Attribute(name, type_id, space_id));

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr = {}, get_type = {}, req = {}\n", fmt::ptr(unwrap(obj_)), get_type, fmt::ptr(req));

    fmt::print(stderr, "Attr Get\n");
    fmt::print("get type = {}\n", get_type);

    // TODO: again, why do we prefer passthru?
    if (unwrap(obj_))
        return VOLBase::attr_get(obj, get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

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
            throw MetadataError(fmt::format("Warning, unknown get_type == {} in attr_get()", get_type));
        }
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::attr_specific(obj_, loc_params, specific_type, dxpl_id, req, arguments);

    // else: TODO

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print("attr = {}, mem_type_id = {}, mem type = {}\n", fmt::ptr(unwrap(attr_)), mem_type_id, Datatype(mem_type_id));

    if (attr_->mdata_obj)
    {
        // save our metadata
        Attribute* a = (Attribute*) attr_->mdata_obj;
        a->write(Datatype(mem_type_id), buf);
    }

    if (unwrap(attr_))
        return VOLBase::attr_write(attr_, mem_type_id, buf, dxpl_id, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;
    fmt::print("close: attr = {}, dxpl_id = {}\n", fmt::ptr(unwrap(attr_)), dxpl_id);

    herr_t retval = 0;

    if (unwrap(attr_))
        retval = VOLBase::attr_close(attr_, dxpl_id, req);

    return retval;
}
