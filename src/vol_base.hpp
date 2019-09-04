#ifndef VOL_BASE_HPP
#define VOL_BASE_HPP

#include <string>

#include "hdf5.h"

// TODO: namespace LowFive

// base class for VOL object
template<class Derived>
struct VOLBase
{
    // The pass through VOL info object
    struct pass_through_t       // formerly known as OUR_pass_through_t
    {
        hid_t   under_vol_id;           // ID for underlying VOL connector
        void*   under_object;           // Info object for underlying VOL connector */
        void*   vol_derived;            // pointer to custom plugin object TODO: not being used currently
    };

    struct info_t       // formerly known as OUR_pass_through_info_t
    {
        hid_t   under_vol_id    = H5VL_NATIVE;      // VOL ID for under VOL
        void*   under_vol_info  = NULL;             // VOL info for under VOL
        void*   vol_derived;                        // pointer to this
    } info;

    unsigned                version;
    int                     value;
    std::string             name;
    H5VL_class_t            connector;
    static hid_t            connector_id;   // static only because of term()

                            VOLBase(unsigned version_, int value_, std::string name_);

    hid_t                   register_plugin();
    static pass_through_t*  new_obj(void *under_obj, hid_t under_vol_id, void* vol_derived);
    static herr_t           free_obj(pass_through_t *obj);

    static herr_t           init(hid_t vipl_id);
    static herr_t           term();

    // info
    static void*            info_copy(const void *_info);
    //void info_cmp()                 {}
    static herr_t           info_free(void *_info);
    //void info_to_str()              {}
    //void str_to_info()              {}

    //// wrap
    //void wrap_get_object()          {}
    //void get_wrap_ctx()             {}
    //void wrap_object()              {}
    //void unwrap_object()            {}
    //void free_wrap_ctx()            {}

    //// attribute
    //void attr_create()              {}
    //void attr_open()                {}
    //void attr_read()                {}
    //void attr_write()               {}
    //void attr_get()                 {}
    //void attr_specific()            {}
    //void attr_optional()            {}
    //void atrr_close()               {}

    // dataset
    static void*           _dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    void*                   dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)         { return 0; }
    //void dset_open()                {}
    //void dset_read()                {}
    //void dset_write()               {}
    //void dset_get()                 {}
    //void dset_specific()            {}
    //void dset_optional()            {}
    //void dset_close()               {}

    //// datatype
    //void dtype_commit()             {}
    //void dtype_open()               {}
    //void dtype_get()                {}
    //void dtype_specific()           {}
    //void dtype_optional()           {}
    //void dtype_close()              {}

    //// file
    static void*           _file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
    void*                   file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)  { return 0; }
    static void*           _file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
    void*                   file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)                   { return 0; }
    //void file_get()                 {}
    //void file_specific()            {}
    //void file_optional()            {}
    static herr_t          _file_close(void *file, hid_t dxpl_id, void **req);
    herr_t                  file_close(void *file, hid_t dxpl_id, void **req)           { return 0; }

    //// group
    //void group_create()             {}
    //void group_open()               {}
    //void group_get()                {}
    //void group_specific()           {}
    //void group_optional()           {}
    //void group_close()              {}

    //// link
    //void link_create()              {}
    //void link_copy()                {}
    //void link_move()                {}
    //void link_get()                 {}
    //void link_specific()            {}
    //void link_optional()            {}

    //// object
    //void obj_open()                 {}
    //void obj_copy()                 {}
    //void obj_get()                  {}
    //void obj_specific()             {}
    //void obj_optional()             {}

    //// request
    //void req_wait()                 {}
    //void req_notify()               {}
    //void req_cancel()               {}
    //void req_specific()             {}
    //void req_optional()             {}
    //void req_free()                 {}
};

#include "vol_base-impl.hpp"

#endif
