#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol_base.hpp"
#include    "../../src/metadata.hpp"

// custom VOL object
// only need to specialize those functions that are custom

// TODO: move inside Vol?
struct ObjectPointers
{
    void*           h5_obj;             // HDF5 object (e.g., dset)
    void*           mdata_obj;          // metadata object (tree node)
};

struct Vol: public VOLBase
{
    using VOLBase::VOLBase;

    std::map<std::string, File*>    files;

    void print_files()
    {
        for (auto& f : files)
        {
            fmt::print("--------------------------------------------------------------------------------\n");
            fmt::print("File {}\n", f.first);
            f.second->print_tree();
            fmt::print("--------------------------------------------------------------------------------\n");
        }
    }

    void*           info_copy(const void *_info) override;

    void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;

    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;

    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    herr_t          group_close(void *grp, hid_t dxpl_id, void **req) override;
};

void*
Vol::info_copy(const void *_info)
{
    fmt::print(stderr, "Copy Info\n");
    return VOLBase::info_copy(_info);
}

void*
Vol::file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
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
Vol::file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;
    File* f = (File*) file_->mdata_obj;

    // FIXME: need to handle this internally, without passthrough
    herr_t res = VOLBase::file_optional(file_->h5_obj, opt_type, dxpl_id, req, arguments);

    return res;
}

herr_t
Vol::file_close(void *file, hid_t dxpl_id, void **req)
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
Vol::dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
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
    string name_(name);
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name_, type_id));

    return (void*)result;
}

herr_t
Vol::dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(dset_->h5_obj), get_type, fmt::ptr(req));

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
Vol::dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;
    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(dset_->h5_obj), mem_space_id, file_space_id);

    // FIXME: need to handle this internally

    return VOLBase::dataset_read(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
Vol::dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(dset_->h5_obj), mem_space_id, file_space_id);

    int m_ndim = H5Sget_simple_extent_ndims(mem_space_id);
    int f_ndim = H5Sget_simple_extent_ndims(file_space_id);

    fmt::print("m_ndim = {}, f_ndim = {}\n", m_ndim, f_ndim);

    // NB: this works only for simple spaces
    std::vector<hsize_t> m_start(m_ndim), m_end(m_ndim);
    H5Sget_select_bounds(mem_space_id, m_start.data(), m_end.data());
    fmt::print("m_start = [{}], m_end = [{}]\n", fmt::join(m_start, ","), fmt::join(m_end, ","));

    std::vector<hsize_t> f_start(f_ndim), f_end(f_ndim);
    H5Sget_select_bounds(file_space_id, f_start.data(), f_end.data());
    fmt::print("f_start = [{}], f_end = [{}]\n", fmt::join(f_start, ","), fmt::join(f_end, ","));

    // save our metadata
    Dataset* ds = (Dataset*) dset_->mdata_obj;
    // FIXME: convert this to triples
    ds->add_dataspace(true, m_ndim, m_start, m_end);
    ds->add_dataspace(false, f_ndim, f_start, f_end);
    ds->set_data(buf);

    return VOLBase::dataset_write(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
Vol::dataset_close(void *dset, hid_t dxpl_id, void **req)
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
Vol::group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
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
Vol::group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = group_close(grp_->h5_obj, dxpl_id, req);

    Group* g = (Group*) grp_->mdata_obj;
    g->remove();
    delete g;

    delete grp_;

    return retval;
}

#endif
