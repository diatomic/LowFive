#include <lowfive/vol-metadata.hpp>

void*
LowFive::MetadataVOL::
info_copy(const void *_info)
{
    fmt::print(stderr, "Copy Info\n");
    return VOLBase::info_copy(_info);
}

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    // create our file metadata
    std::string name_(name);
    File* f = new File(name_);
    files.emplace(name_, f);

    ObjectPointers* obj_ptrs = new ObjectPointers;
    obj_ptrs->mdata_obj = f;
    obj_ptrs->h5_obj = VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;
    File* f = (File*) file_->mdata_obj;

    fmt::print(stderr, "file_optional: opt_type = {}\n", opt_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    // FIXME: need to handle this internally, without passthrough
    herr_t res = VOLBase::file_optional(file_->h5_obj, opt_type, dxpl_id, req, arguments);

    return res;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;
    File* f = (File*) file_->mdata_obj;

    herr_t res = VOLBase::file_close(file_->h5_obj, dxpl_id, req);

    delete file_;

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

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = new ObjectPointers;

    result->h5_obj = VOLBase::dataset_create(obj_->h5_obj, loc_params, name,
            lcpl_id,  type_id,
            space_id, dcpl_id,
            dapl_id,  dxpl_id, req);

    // add the dataset to our file metadata
    std::string name_(name);
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name_, type_id, space_id));

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(dset_->h5_obj), get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    //herr_t result = VOLBase::dataset_get(dset_->h5_obj, get_type, dxpl_id, req, arguments);
    //fmt::print("result = {}\n", result);

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

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    fmt::print("dset = {}\nmem_space_id = {}\nfile_space_id = {}\n",
               fmt::ptr(dset_->h5_obj),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    // get dataset from our metadata and dataspace being read
    Dataset*    ds = (Dataset*) dset_->mdata_obj;
    Dataspace   rs(file_space_id);

    // sanity check that the datatype and dimensionality being read matches the metadata
    // TODO: HDF5 allows datatypes to not match and takes care of the conversion;
    //       eventually, we need to support this functionality as well
    if (ds->type.dtype_class != convert_type_class(H5Tget_class(mem_type_id)) ||
            ds->type.dtype_size != 8 * H5Tget_size(mem_type_id))
        throw MetadataError(fmt::format("Error: dataset_read(): type mismatch"));
    if (rs.dim != ds->space.dim)
        throw MetadataError(fmt::format("Error: dataset_read(): dim mismatch"));

    for (auto& dt : ds->data)                               // for all the data triples in the metadata dataset
    {
        Dataspace& fs = dt.file;

        hid_t mem_dst = Dataspace::project_intersection(file_space_id, mem_space_id, fs.id);
        hid_t mem_src = Dataspace::project_intersection(fs.id, dt.memory.id, file_space_id);

        Dataspace dst(mem_dst, true);
        Dataspace src(mem_src, true);

        fmt::print("dst = {}\n", dst);
        fmt::print("src = {}\n", src);
    }   // for all data triples

    // TODO: eventually return here and don't pass through below
//     return 0;

    return VOLBase::dataset_read(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})\n",
               fmt::ptr(dset_->h5_obj),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    // save our metadata
    Dataset* ds = (Dataset*) dset_->mdata_obj;
    ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);

    return VOLBase::dataset_write(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    fmt::print("close: dset = {}, dxpl_id = {}\n", fmt::ptr(dset_->h5_obj), dxpl_id);

    herr_t retval = VOLBase::dataset_close(dset_->h5_obj, dxpl_id, req);

    Dataset* ds = (Dataset*) dset_->mdata_obj;

    delete dset_;

    return retval;
}

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    ObjectPointers* result = new ObjectPointers;
    result->h5_obj = VOLBase::group_create(obj_->h5_obj, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Group(name));

    return result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = VOLBase::group_close(grp_->h5_obj, dxpl_id, req);

    Group* g = (Group*) grp_->mdata_obj;

    delete grp_;

    return retval;
}

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Attr Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    ObjectPointers* result = new ObjectPointers;
    result->h5_obj = VOLBase::attr_create(obj_->h5_obj, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
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

    fmt::print("attr = {}, get_type = {}, req = {}\n", fmt::ptr(obj_->h5_obj), get_type, fmt::ptr(req));

    fmt::print(stderr, "Attr Get\n");
    fmt::print("get type = {}\n", get_type);

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

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print("attr = {}, mem_type_id = {}, mem type = {}\n", fmt::ptr(attr_->h5_obj), mem_type_id, Datatype(mem_type_id));

    // save our metadata
    Attribute* a = (Attribute*) attr_->mdata_obj;
    a->write(Datatype(mem_type_id), buf);

    return VOLBase::attr_write(attr_->h5_obj, mem_type_id, buf, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;
    fmt::print("close: attr = {}, dxpl_id = {}\n", fmt::ptr(attr_->h5_obj), dxpl_id);

    herr_t retval = VOLBase::attr_close(attr_->h5_obj, dxpl_id, req);

    Attribute* a = (Attribute*) attr_->mdata_obj;

    delete attr_;

    return retval;
}
