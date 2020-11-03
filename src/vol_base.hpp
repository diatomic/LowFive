#ifndef VOL_BASE_HPP
#define VOL_BASE_HPP

#include <string>

#include "hdf5.h"

// TODO: namespace LowFive

// base class for VOL object
struct VOLBase
{
    // automatically manage reference count
    struct UnderObject
    {
        hid_t   under_vol_id;                   // ID for underlying VOL connector

                UnderObject(hid_t uvi):
                    under_vol_id(uvi)           { H5Iinc_ref(under_vol_id); }
                ~UnderObject()                  { hid_t err_id; err_id = H5Eget_current_stack(); H5Idec_ref(under_vol_id); H5Eset_current_stack(err_id); }
    };

    // The pass through VOL info object
    struct pass_through_t: public UnderObject
    {
        using UnderObject::UnderObject;
                            pass_through_t(void* under_object_, hid_t under_vol_id_, VOLBase* vol_):
                                UnderObject(under_vol_id_), under_object(under_object_), vol(vol_)      {}

        pass_through_t*     create(void* o)                     { auto x = new pass_through_t(o, this->under_vol_id, vol); return x; }
        static void         destroy(pass_through_t* x)          { delete x; }       // TODO: do we really need this?

        void*               under_object;                       // Info object for underlying VOL connector
        VOLBase*            vol;                                // pointer to this
    };

    struct info_t
    {
        hid_t               under_vol_id    = H5VL_NATIVE;      // VOL ID for under VOL
        void*               under_vol_info  = NULL;             // VOL info for under VOL
        VOLBase*            vol;                                // pointer to this
    } info;

    unsigned                version;
    int                     value;
    std::string             name;
    H5VL_class_t            connector;
    static hid_t            connector_id;   // static only because of term()

                            VOLBase(unsigned version_, int value_, std::string name_);

    hid_t                   register_plugin();

    static herr_t           _init(hid_t vipl_id);
    static herr_t           _term();

    // info
    static void*           _info_copy(const void *_info);
    virtual void*           info_copy(const void *_info);
    //void info_cmp()                 {}
    static herr_t          _info_free(void *_info);
    virtual herr_t          info_free(void *_info);
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
    virtual void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    //void dset_open()                {}
    static herr_t          _dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
    virtual herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
    static herr_t          _dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
    virtual herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
    static herr_t          _dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    //void dset_specific()            {}
    //void dset_optional()            {}
    static herr_t          _dataset_close(void *dset, hid_t dxpl_id, void **req);
    virtual herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req);

    //// datatype
    //void dtype_commit()             {}
    //void dtype_open()               {}
    //void dtype_get()                {}
    //void dtype_specific()           {}
    //void dtype_optional()           {}
    //void dtype_close()              {}

    //// file
    static void*           _file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
    virtual void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
    static void*           _file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
    virtual void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
    static herr_t          _file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    //void file_get()                 {}
    //void file_specific()            {}
    static herr_t          _file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _file_close(void *file, hid_t dxpl_id, void **req);
    virtual herr_t          file_close(void *file, hid_t dxpl_id, void **req);

    //// group
    static void*           _group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    virtual void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    //void group_open()               {}
    //void group_get()                {}
    //void group_specific()           {}
    //void group_optional()           {}
    static herr_t          _group_close(void *grp, hid_t dxpl_id, void **req);
    virtual herr_t          group_close(void *grp, hid_t dxpl_id, void **req);

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

    //// Container/connector introspection
    static herr_t          _introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
    virtual herr_t          introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
    static herr_t          _introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);
    virtual herr_t          introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);

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
