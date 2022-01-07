#include <lowfive/vol-metadata.hpp>
#include <cassert>
#include <lowfive/metadata/dummy.hpp>

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

    fmt::print("file_create: obj_ptrs = {} [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(obj_ptrs), fmt::ptr(obj_ptrs->h5_obj), fmt::ptr(obj_ptrs->mdata_obj), dxpl_id);

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
    fmt::print(stderr, "MetadataVOL::file_open()\n");
    ObjectPointers* obj_ptrs = nullptr;

    // find the file in the VOL
    void* mdata = nullptr;
    std::string name_(name);
    auto it = files.find(name_);
    if (it != files.end())
        mdata = it->second;
    else
    {
        auto* f = new DummyFile(name);
        files.emplace(name, f);
        mdata = f;
    }

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

    fmt::print("file_get: file = {} [h5_obj {} mdata_obj {}], get_type = {} req = {} dxpl_id = {}\n",
            fmt::ptr(file_), fmt::ptr(file_->h5_obj), fmt::ptr(file_->mdata_obj), get_type, fmt::ptr(req), dxpl_id);
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
//     fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
//     fmt::print("data type = {}\n", Datatype(type_id));
//     fmt::print("data space = {}\n", Dataspace(space_id));

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "Dataset Create\n");
    fmt::print("dataset_create: parent obj = {} [h5_obj {} mdata_obj {}], name = {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), name);

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

//     // from tom-dev (DEPRECATE)
//     if (vol_properties.memory)                                              // add the dataset to our file metadata
//     {
//         // trace object back to root to build full path and file name
//         std::string name_(name);
//         std::string full_path;
//         std::string filename;
//         backtrack_name(name_, static_cast<Object*>(obj_->mdata_obj), filename, full_path);
//
//         // check the ownership map for the full path name and file name
//         Dataset::Ownership own = Dataset::Ownership::user;
//         for (auto& o : data_owners)
//         {
//             // o.filename and o.full_path can have wildcards '*' and '?'
//             if (match(o.filename.c_str(), filename.c_str()) && match(o.full_path.c_str(), full_path.c_str()))
//             {
//                 own = o.ownership;
//                 break;
//             }
//         }
//
//         // add the dataset
//         result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name_, type_id, space_id, own));
//
//     }

    fmt::print(stderr, "dataset_create: created result {} result->h5_obj {} result->mdata_obj {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "dataset_open {} {}\n", fmt::ptr(obj_->mdata_obj), fmt::ptr(obj_->h5_obj));

    // trace object back to root to build full path and file name
    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    if (match_any(filepath, passthru))
        result = (ObjectPointers*) VOLBase::dataset_open(obj_, loc_params, name, dapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    if (match_any(filepath, memory))
        // find the dataset in our file metadata
        result->mdata_obj = parent->search(name);

    if (!result->mdata_obj)
        result->mdata_obj = parent->add_child(new DummyDataset(name));

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
        if (file_space_id && Dataspace(file_space_id).dim != ds->space.dim)
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

    fmt::print(stderr, "Dataset Close\n");
    fmt::print(stderr, "dataset_close: dset = {} = [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(dset_), fmt::ptr(dset_->h5_obj), fmt::ptr(dset_->mdata_obj), dxpl_id);
    if (dset_->mdata_obj)
        fmt::print(stderr, "closing dataset name {}\n", static_cast<Object*>(dset_->mdata_obj)->name);

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

    return (void*)result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = (ObjectPointers*) VOLBase::group_open(obj_, loc_params, name, gapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the group in our file metadata
    std::string name_(name);
    result->mdata_obj = parent->search(name_);
    if (!result->mdata_obj)
        result->mdata_obj = parent->add_child(new DummyGroup(name_));

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

    fmt::print("created attribute object = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return result;
}

void*
LowFive::MetadataVOL::
attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "Attr Open\n");
    fmt::print("attr_open obj = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), name);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    if (match_any(filepath, passthru))
        result = (ObjectPointers*) VOLBase::attr_open(obj_, loc_params, name, aapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the attribute in our file metadata
    std::string name_(name);
    if (match_any(filepath, memory))
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_);

    fmt::print("attr_open search result = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return (void*)result;
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

// helper function for attr_specific()
void
LowFive::MetadataVOL::
attr_exists(void *obj, va_list arguments)
{
    ObjectPointers* obj_    = (ObjectPointers*) obj;

    const char *attr_name   = va_arg(arguments, const char *);
    htri_t *    ret         = va_arg(arguments, htri_t *);

    // check direct children of the parent object (NB, not full search all the way down the tree)
    *ret = 0;           // not found
    for (auto& c : static_cast<Object*>(obj_->mdata_obj)->children)
    {
        if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name)
        {
            *ret = 1;   // found
            break;
        }
    }
    if (*ret)
        fmt::print(stderr, "Found attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
    else
        fmt::print(stderr, "Did not find attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
}

// helper function for attr_specific()
void
LowFive::MetadataVOL::
attr_iter(void *obj, va_list arguments)
{
    ObjectPointers* obj_        = (ObjectPointers*) obj;
    Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

    // copied from HDF5 H5VLnative_attr.c, H5VL__native_attr_specific()
    H5_index_t      idx_type = (H5_index_t)va_arg(arguments, int);
    H5_iter_order_t order    = (H5_iter_order_t)va_arg(arguments, int);
    hsize_t *       idx      = va_arg(arguments, hsize_t *);
    H5A_operator2_t op       = va_arg(arguments, H5A_operator2_t);
    void *          op_data  = va_arg(arguments, void *);

    // get object type in HDF format and use that to get an HDF hid_t to the object
    std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
    int obj_type = h5_types[static_cast<int>(mdata_obj->type)];
    // TODO: following causes the closing of the dataset to crash
    hid_t obj_loc_id = H5VLwrap_register(obj, static_cast<H5I_type_t>(obj_type));
    // TODO: following does not solve the crash when closing the dataset
//     int refs = H5Iinc_ref(obj_loc_id);

    // debug
    fmt::print(stderr, "attr_iter(): obj_type {} H5I_DATASET {} obj_loc_id {} name {}\n",
            obj_type, H5I_DATASET, obj_loc_id, mdata_obj->name);

    // info for attribute, defined in HDF5 H5Apublic.h  TODO: assigned with some defaults, not sure if they're corrent
    H5A_info_t ainfo;
    ainfo.corder_valid  = true;                     // whether creation order is valid
    ainfo.corder        = 0;                        // creation order (TODO: no idea what this is)
    ainfo.cset          = H5T_CSET_ASCII;           // character set of attribute names
    ainfo.data_size =                               // size of raw data (bytes)
        static_cast<Attribute*>(mdata_obj)->space.size() * static_cast<Attribute*>(mdata_obj)->type.dtype_size;

    // check direct children of the parent object, not full search all the way down the tree
    bool found = false;         // some attributes were found

    // TODO: currently ignores the iteration order, increment direction, and current index
    // just blindly goes through all the attributes in the order they were created
    for (auto& c : mdata_obj->children)
    {
        if (c->type == LowFive::ObjectType::Attribute)
        {
            found = true;

            fmt::print(stderr, "Found attribute {} as a child of the parent {}\n", c->name, mdata_obj->name);

            fmt::print(stderr, "*** ------------------- ***\n");
            fmt::print(stderr, "Warning: operating on attribute not fully implemented yet.\n");
            fmt::print(stderr, "Ignoring object location, name, attribute info, attribute order, increment direction, current index.\n");
            fmt::print(stderr, "Basically just stepping through all attributes of the object in the order they were created.\n");
            fmt::print(stderr, "*** ------------------- ***\n");

            // make the application callback, copied from H5Aint.c, H5A__attr_iterate_table()
            herr_t retval = (op)(obj_loc_id, c->name.c_str(), &ainfo, op_data);
            if (retval > 0)
            {
                fmt::print(stderr, "Terminating iteration because operator returned > 0 value, indicating user-defined early termination\n");
                break;
            }
            else if (retval < 0)
            {
                fmt::print(stderr, "Terminating iteration because operator returned < 0 value, indicating user-defined failure\n");
                break;
            }
        }   // child is type attribute
    }   // for all children
    if (!found)
    {
        fmt::print(stderr, "Did not find any attributes as direct children of the parent {} when trying to iterate over attributes\n.", mdata_obj->name);
    }
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr_specific obj = {} = [h5_obj {} mdata_obj {}] specific_type = {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), specific_type);
    fmt::print("specific types H5VL_ATTR_DELETE = {} H5VL_ATTR_EXISTS = {} H5VL_ATTR_ITER = {} H5VL_ATTR_RENAME = {}\n",
            H5VL_ATTR_DELETE, H5VL_ATTR_EXISTS, H5VL_ATTR_ITER, H5VL_ATTR_RENAME);

    // trace object back to root to build full path and file name
    auto name = static_cast<Object*>(obj_->mdata_obj)->name;
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::attr_specific(obj_, loc_params, specific_type, dxpl_id, req, arguments);

    else if (match_any(filepath, memory))
    {
        switch(specific_type)
        {
            case H5VL_ATTR_DELETE:                      // H5Adelete(_by_name/idx)
            {
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_DELETE not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            case H5VL_ATTR_EXISTS:                      // H5Aexists(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_EXISTS\n");
                attr_exists(obj, arguments);

                break;
            }
            case H5VL_ATTR_ITER:                        // H5Aiterate(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_ITER\n");
                attr_iter(obj, arguments);

                break;
            }
            case H5VL_ATTR_RENAME:                     // H5Arename(_by_name)
            {
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_RENAME not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            default:
                throw MetadataError(fmt::format("Unknown specific_type in LowFive::MetadataVOL::attr_specific()"));
        }
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print(stderr, "Attr Write\n");
    fmt::print("attr = {} = [h5_obj {} mdata_obj {}], mem_type_id = {}, mem type = {}\n",
            fmt::ptr(attr_), fmt::ptr(attr_->h5_obj), fmt::ptr(attr_->mdata_obj), mem_type_id, Datatype(mem_type_id));

    if (attr_->mdata_obj)
    {
        // save our metadata
        Attribute* a = (Attribute*) attr_->mdata_obj;
        // skip an attribute that wasn't created or otherwise doesn't have a valide metadata object
//         if (a)
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

    fmt::print(stderr, "Attr Close\n");
    fmt::print("close: attr = {} = [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(attr_), fmt::ptr(attr_->h5_obj), fmt::ptr(attr_->mdata_obj), dxpl_id);

    herr_t retval = 0;

    if (unwrap(attr_))
        retval = VOLBase::attr_close(attr_, dxpl_id, req);

    return retval;
}
