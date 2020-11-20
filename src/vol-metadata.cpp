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

    files.erase(f->name);
    f->remove();        // redundant, since no parent
    delete f;

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

    namespace h5 = HighFive;
    auto class_string = h5::type_class_string(h5::convert_type_class(H5Tget_class(type_id)));
    fmt::print("data type = {}{}\n", class_string, 8 * H5Tget_size(type_id));

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = new ObjectPointers;

    result->h5_obj = VOLBase::dataset_create(obj_->h5_obj, loc_params, name,
            lcpl_id,  type_id,
            space_id, dcpl_id,
            dapl_id,  dxpl_id, req);

    // add the dataset to our file metadata
    std::string name_(name);
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name_, type_id));

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

    // FIXME: need to handle this internally, without passthrough
    herr_t result = VOLBase::dataset_get(dset_->h5_obj, get_type, dxpl_id, req, arguments);

    fmt::print("result = {}\n", result);

    if (get_type == H5VL_DATASET_GET_SPACE)
    {
        fmt::print("GET_SPACE\n");
        hid_t *ret = va_arg(args, hid_t*);
        fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);

        hid_t space_id = *ret;
        int ndim = H5Sget_simple_extent_ndims(space_id);
        fmt::print("ndim = {}\n", ndim);

        // NB: this works only for simple spaces
        std::vector<hsize_t> start(ndim), end(ndim);
        H5Sget_select_bounds(space_id, start.data(), end.data());
        fmt::print("start = [{}], end = [{}]\n", fmt::join(start, ","), fmt::join(end, ","));
    } else if (get_type == H5VL_DATASET_GET_TYPE)
    {
        fmt::print("GET_TYPE\n");
        hid_t *ret = va_arg(args, hid_t*);
        fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);

        hid_t type_id = *ret;
        namespace h5 = HighFive;
        auto class_string = h5::type_class_string(h5::convert_type_class(H5Tget_class(type_id)));
        fmt::print("data type = {}{}\n", class_string, 8 * H5Tget_size(type_id));
    } else
    {
        fmt::print(stderr, "Warning, unknown get_type == {} in dataset_get()", get_type);
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(dset_->h5_obj), mem_space_id, file_space_id);

    // FIXME: need to handle this internally

    return VOLBase::dataset_read(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(dset_->h5_obj), mem_space_id, file_space_id);

    // save our metadata
    Dataset* ds = (Dataset*) dset_->mdata_obj;
    ds->write(Dataspace(mem_space_id), Dataspace(file_space_id), buf);

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
    ds->remove();
    delete ds;

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
    g->remove();
    delete g;

    delete grp_;

    return retval;
}

