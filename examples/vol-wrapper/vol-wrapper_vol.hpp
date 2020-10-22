#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol_base.hpp"

#include <highfive/H5DataType.hpp>

// custom VOL object
// only need to specialize those functions that are custom

struct Vol: public VOLBase<Vol>
{
    using VOLBase<Vol>::VOLBase;

    static herr_t   init(hid_t vipl_id)             { fmt::print(stderr, "Hello Vol\n"); return 0; }
    static herr_t   term()                          { fmt::print(stderr, "Goodbye Vol\n"); return 0; }

    void*           info_copy(const void *_info)    { fmt::print(stderr, "Copy Info\n"); return 0; }
    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
};


void*
Vol::dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
                    const char *name,
                    hid_t lcpl_id,  hid_t type_id,
                    hid_t space_id, hid_t dcpl_id,
                    hid_t dapl_id,  hid_t dxpl_id, void **req)
{
    fmt::print(stderr, "Dataset Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    namespace h5 = HighFive;
    auto class_string = h5::type_class_string(h5::convert_type_class(H5Tget_class(type_id)));
    fmt::print("data type = {}{}\n", class_string, 8 * H5Tget_size(type_id));

    return 0;
}

herr_t
Vol::dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    fmt::print("get_type = {}\n", get_type);
    return 0;
}

void*
Vol::group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
    return 0;
}

#endif
