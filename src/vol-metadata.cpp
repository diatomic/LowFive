#include <lowfive/vol-metadata.hpp>

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    void* mdata = nullptr;
    if (vol_properties.memory)
    {
        // create our file metadata
        std::string name_(name);
        File* f = new File(name_);
        files.emplace(name_, f);
        mdata = f;
    }

    pass_through_t* result = nullptr;
    if (vol_properties.passthru)
        result = (pass_through_t*) VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);

    if (!result)
        result = new pass_through_t(nullptr, info->under_vol_id, info->vol);

    result->extra = mdata;

    return result;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t* file_ = (pass_through_t*) file;

    fmt::print(stderr, "file_optional: opt_type = {}\n", opt_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    herr_t res = 0;
    if (vol_properties.passthru && file_->under_object)
        res = VOLBase::file_optional(file_, opt_type, dxpl_id, req, arguments);

    return res;
}

void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    void* mdata = nullptr;
    if (vol_properties.memory)
    {
        // create our file metadata
        std::string name_(name);
        File* f = new File(name_);
        files.emplace(name_, f);
        mdata = f;
    }

    pass_through_t* result = nullptr;
    if (vol_properties.passthru && ! vol_properties.memory)
        result = (pass_through_t*) VOLBase::file_open(name, flags, fapl_id, dxpl_id, req);

    if (!result)
        result = new pass_through_t(nullptr, info->under_vol_id, info->vol);

    result->extra = mdata;

    return result;
}

herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t* file_ = (pass_through_t*) file;

    va_list args;
    va_copy(args,arguments);

    fmt::print("file = {}, get_type = {}, req = {}\n", fmt::ptr(file_), get_type, fmt::ptr(req));
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::file_get(file_, get_type, dxpl_id, req, arguments);

    // else: TODO

    return result;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    pass_through_t* file_ = (pass_through_t*) file;

    herr_t res = 0;

    if (vol_properties.passthru && file_->under_object)
        res = VOLBase::file_close(file_, dxpl_id, req);

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
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
    fmt::print("data type = {}\n", Datatype(type_id));
    fmt::print("data space = {}\n", Dataspace(space_id));

    pass_through_t* obj_ = (pass_through_t*) obj;
    pass_through_t* result = nullptr;

    fmt::print("create: dset = {}, dxpl_id = {}\n", fmt::ptr(obj_), dxpl_id);

    if (vol_properties.passthru)
        result = (pass_through_t*) VOLBase::dataset_create(obj_, loc_params, name,
                lcpl_id,  type_id, space_id, dcpl_id, dapl_id,  dxpl_id, req);

    if (!result)
        result = obj_->create(nullptr);

    if (vol_properties.memory)                                              // add the dataset to our file metadata
    {
        // trace object back to root to build full path and file name
        std::string name_(name);
        std::string full_path;
        std::string filename;
        backtrack_name(name_, static_cast<Object*>(obj_->extra), filename, full_path);

        // check the ownership map for the full path name and file name
        Dataset::Ownership own = Dataset::Ownership::user;
        for (auto& o : data_owners)
        {
            // o.filename and o.full_path can have wildcards '*' and '?'
            if (match(o.filename.c_str(), filename.c_str()) && match(o.full_path.c_str(), full_path.c_str()))
            {
                own = o.ownership;
                break;
            }
        }

        // debug
//         fmt::print("dataset_create(): filename {} full_path {} own {}\n", filename, full_path, own);

        // add the dataset
        result->extra = static_cast<Object*>(obj_->extra)->add_child(new Dataset(name_, type_id, space_id, own));
    }

    return (void*)result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    pass_through_t* obj_ = (pass_through_t*) obj;
    pass_through_t* result = nullptr;

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    if (vol_properties.passthru && !vol_properties.memory)
        result = (pass_through_t*) VOLBase::dataset_open(obj_, loc_params, name, dapl_id, dxpl_id, req);

    if (!result)
        result = obj_->create(nullptr);

    // find the dataset in our file metadata
    std::string name_(name);
    if (vol_properties.memory)
        result->extra = static_cast<Object*>(obj_->extra)->search(name_);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t* dset_ = (pass_through_t*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(dset_), get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::dataset_get(dset_, get_type, dxpl_id, req, arguments);

    if (vol_properties.memory)
    {
        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Dataset*>(dset_->extra)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Dataset*>(dset_->extra)->type;

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
    pass_through_t* dset_ = (pass_through_t*) dset;

    fmt::print("dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}\n",
               fmt::ptr(dset_),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (vol_properties.memory)
    {
        Dataset* ds = (Dataset*) dset_->extra;              // dataset from our metadata

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

    if (vol_properties.passthru)
        return VOLBase::dataset_read(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    pass_through_t* dset_ = (pass_through_t*) dset;

    fmt::print("dset = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})\n",
               fmt::ptr(dset_),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (vol_properties.memory)
    {
        // save our metadata
        Dataset* ds = (Dataset*) dset_->extra;
        ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);
    }

    if (vol_properties.passthru)
        return VOLBase::dataset_write(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    pass_through_t* dset_ = (pass_through_t*) dset;
    fmt::print("close: dset = {}, dxpl_id = {}\n", fmt::ptr(dset_), dxpl_id);

    herr_t retval = 0;

    // close from the file if not using metadata in memory
    // if both memory and passthru are enabled, close from file (producer) only
    if (vol_properties.passthru && !vol_properties.memory)
        retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    else if (vol_properties.passthru && vol_properties.memory)
    {
        if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->extra))
            retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    }

    return retval;
}

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    pass_through_t* obj_ = (pass_through_t*) obj;

    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    pass_through_t* result = nullptr;
    if (vol_properties.passthru)
        result = (pass_through_t*) VOLBase::group_create(obj_, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);

    if (!result)
        result = obj_->create(nullptr);

    // add group to our metadata
    if (vol_properties.memory)
        result->extra = static_cast<Object*>(obj_->extra)->add_child(new Group(name));

    return result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    pass_through_t* obj_ = (pass_through_t*) obj;
    pass_through_t* result = nullptr;

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    if (vol_properties.passthru && !vol_properties.memory)
        result = (pass_through_t*) VOLBase::group_open(obj_, loc_params, name, gapl_id, dxpl_id, req);

    if (!result)
        obj_->create(nullptr);

    // find the group in our file metadata
    std::string name_(name);
    if (vol_properties.memory)
        result->extra = static_cast<Object*>(obj_->extra)->search(name_);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    pass_through_t* grp_ = (pass_through_t*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = 0;

    if (vol_properties.passthru)
        retval = VOLBase::group_close(grp_, dxpl_id, req);

    // TODO: why create a pointer that isn't used?
    if (vol_properties.memory)
        Group* g = (Group*) grp_->extra;

    return retval;
}

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    pass_through_t* obj_ = (pass_through_t*) obj;

    fmt::print(stderr, "Attr Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    pass_through_t* result = nullptr;
    if (vol_properties.passthru)
        result = (pass_through_t*) VOLBase::attr_create(obj_, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);

    // add attribute to our metadata
    if (vol_properties.memory)
        result->extra = static_cast<Object*>(obj_->extra)->add_child(new Attribute(name, type_id, space_id));

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t* obj_ = (pass_through_t*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr = {}, get_type = {}, req = {}\n", fmt::ptr(obj_), get_type, fmt::ptr(req));

    fmt::print(stderr, "Attr Get\n");
    fmt::print("get type = {}\n", get_type);

    // FIXME: this is missing the passthru code path

    if (vol_properties.memory)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Attribute*>(obj_->extra)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Attribute*>(obj_->extra)->type;

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
    pass_through_t* obj_ = (pass_through_t*) obj;

    va_list args;
    va_copy(args,arguments);

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::attr_specific(obj_, loc_params, specific_type, dxpl_id, req, arguments);

    // else: TODO

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    pass_through_t* attr_ = (pass_through_t*) attr;

    fmt::print("attr = {}, mem_type_id = {}, mem type = {}\n", fmt::ptr(attr_), mem_type_id, Datatype(mem_type_id));

    if (vol_properties.memory)
    {
        // save our metadata
        Attribute* a = (Attribute*) attr_->extra;
        a->write(Datatype(mem_type_id), buf);
    }

    if (vol_properties.passthru)
        return VOLBase::attr_write(attr_, mem_type_id, buf, dxpl_id, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    pass_through_t* attr_ = (pass_through_t*) attr;
    fmt::print("close: attr = {}, dxpl_id = {}\n", fmt::ptr(attr_), dxpl_id);

    herr_t retval = 0;

    if (vol_properties.passthru)
        retval = VOLBase::attr_close(attr_, dxpl_id, req);

    // TODO: why create a pointer that isn't used?
    if (vol_properties.memory)
        Attribute* a = (Attribute*) attr_->extra;

    return retval;
}
