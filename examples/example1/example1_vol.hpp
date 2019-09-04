#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol_base.hpp"

// custom VOL object
// only need to specialize those functions that are custom

struct Vol: public VOLBase<Vol>
{
    using VOLBase<Vol>::VOLBase;

    static herr_t   init(hid_t vipl_id)             { fmt::print(stderr, "Hello Vol\n"); return 0; }
    static herr_t   term()                          { fmt::print(stderr, "Goodbye Vol\n"); return 0; }

    static void*    info_copy(const void *_info)    { fmt::print(stderr, "Copy Info\n"); return 0; }
    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
                                                    { fmt::print(stderr, "Dataset Create\n"); return 0; }
};

#endif
