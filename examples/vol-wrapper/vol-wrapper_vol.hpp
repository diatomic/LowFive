#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol_base.hpp"

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

    void*           info_copy(const void *_info);
    void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req);
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
    metadata = new FileMetadata(name_);

    return VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
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

    ObjectPointers* obj_ptrs = new ObjectPointers;

    obj_ptrs->h5_obj = VOLBase::dataset_create(obj, loc_params, name,
            lcpl_id,  type_id,
            space_id, dcpl_id,
            dapl_id,  dxpl_id, req);

    // add the dataset to our file metadata
    string name_(name);
    obj_ptrs->mdata_obj = metadata->add_node(NULL, new Object((ObjectType)type_id, name_));

    return (void*)obj_ptrs;
}

herr_t
Vol::dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ptrs = (ObjectPointers*)dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(obj_ptrs->h5_obj), get_type, fmt::ptr(req));

    herr_t result = VOLBase::dataset_get(obj_ptrs->h5_obj, get_type, dxpl_id, req, arguments);

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
    }

    return result;
}

herr_t
Vol::dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* obj_ptrs = (ObjectPointers*)dset;
    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(obj_ptrs->h5_obj), mem_space_id, file_space_id);

    return VOLBase::dataset_read(obj_ptrs->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
Vol::dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* obj_ptrs = (ObjectPointers*)dset;

    fmt::print("dset = {}, mem_space_id = {}, file_space_id = {}\n", fmt::ptr(obj_ptrs->h5_obj), mem_space_id, file_space_id);

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
    TreeNode<Object>* node = static_cast<TreeNode<Object>*>(obj_ptrs->mdata_obj);
    Dataspace m_dataspace, f_dataspace;
    m_dataspace.dim     = m_ndim;
    m_dataspace.min.resize(m_ndim);
    m_dataspace.max.resize(m_ndim);
    f_dataspace.dim     = f_ndim;
    f_dataspace.min.resize(f_ndim);
    f_dataspace.max.resize(f_ndim);
    for (auto i = 0; i < m_ndim; i++)
    {
        m_dataspace.min[i] = m_start[i];
        m_dataspace.max[i] = m_end[i];
    }
    for (auto i = 0; i < f_ndim; i++)
    {
        f_dataspace.min[i] = f_start[i];
        f_dataspace.max[i] = f_end[i];
    }
    node->node_data->m_dataspaces.push_back(m_dataspace);
    node->node_data->f_dataspaces.push_back(f_dataspace);

    return VOLBase::dataset_write(obj_ptrs->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
Vol::dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ptrs = (ObjectPointers*)dset;
    fmt::print("close: dset = {}, dxpl_id = {}\n", fmt::ptr(obj_ptrs->h5_obj), dxpl_id);

    herr_t retval = VOLBase::dataset_close(obj_ptrs->h5_obj, dxpl_id, req);

    delete obj_ptrs;
    return retval;
}

void*
Vol::group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
    return VOLBase::group_create(obj, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
}

#endif
