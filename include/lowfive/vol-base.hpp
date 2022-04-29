#ifndef LOWFIVE_VOL_BASE_HPP
#define LOWFIVE_VOL_BASE_HPP

#include <string>
#include <memory>

#include <hdf5.h>
#include <lowfive/log.hpp>

namespace LowFive
{

// base class for VOL object
struct VOLBase
{
    //// automatically manage reference count
    //struct UnderObject
    //{
    //    hid_t   under_vol_id;                   // ID for underlying VOL connector

    //            UnderObject(hid_t uvi):
    //                under_vol_id(uvi)           { H5Iinc_ref(under_vol_id); }
    //            ~UnderObject()                  { hid_t err_id; err_id = H5Eget_current_stack(); H5Idec_ref(under_vol_id); H5Eset_current_stack(err_id); }
    //};

    // The pass through VOL info object;
    // it increments the reference count for the under_vold_id on creation and decrements on destruction
    struct pass_through_t
    {
                            pass_through_t(void* under_object_, hid_t under_vol_id_, VOLBase* vol_):
                                under_object(under_object_), under_vol_id(under_vol_id_), vol(vol_)      { H5Iinc_ref(under_vol_id_); }

        pass_through_t*     create(void* o)                     { auto x = new pass_through_t(o, under_vol_id, vol); return x; }
        static void         destroy(pass_through_t* x)
        {
            x->vol->drop(x->under_object);
            destroy_wrapper(x);
        }
        static void         destroy_wrapper(pass_through_t* x)
        {
            hid_t err_id = H5Eget_current_stack();
            H5Idec_ref(x->under_vol_id);
            H5Eset_current_stack(err_id);
            delete x;
        }

        void*               under_object;
        hid_t               under_vol_id;
        VOLBase*            vol;                                // pointer to this
    };

    // The pass through VOL wrapper context
    struct pass_through_wrap_ctx_t {
        hid_t       under_vol_id;          // VOL ID for under VOL
        void        *under_wrap_ctx;       // Object wrapping context for under VOL
        VOLBase*    vol;                   // pointer to this
    };

    struct info_t
    {
        hid_t               under_vol_id;      // VOL ID for under VOL
        void*               under_vol_info;             // VOL info for under VOL
        static VOLBase*     vol;                                // pointer to this;
                                                                // DM: made it static to make the environment-variable loading of VOL work;
                                                                //     very unfortunate design, but it's the only way I can find to match HDF5 VOL design
    };

    std::shared_ptr<spdlog::logger> log;

    static info_t* info;     // this needs to be static, since in the environment variable case, we need to configure it from _str_to_info

    unsigned                version;
    int                     value;
    std::string             name;
    static H5VL_class_t     connector;      // need this static for H5PL_* functions (for automatic plugin loading via environment variables)
    static hid_t            connector_id;   // static only because of term()

                            VOLBase();
                            ~VOLBase();

    virtual void            drop(void* p)       {}

    hid_t                   register_plugin();

    static herr_t           _init(hid_t vipl_id);
    static herr_t           _term();

    // info
    static void*           _info_copy(const void *_info);
    //void info_cmp()                 {}
    static herr_t          _info_free(void *_info);
    static herr_t          _info_to_str(const void *info, char **str);
    //virtual herr_t          info_to_str(const void *info, char **str);
    static herr_t          _str_to_info(const char *str, void **info);
    //virtual herr_t          str_to_info(const char *str, void **info);

    // wrap
    static void*           _wrap_get_object(const void *obj);
    virtual void*           wrap_get_object(void *obj);
    static herr_t          _get_wrap_ctx(const void *obj, void **wrap_ctx);
    virtual herr_t          get_wrap_ctx(void *obj, void **wrap_ctx);
    static void*           _wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx);
    virtual void*           wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx);
    static void*           _unwrap_object(void *obj);
    virtual void*           unwrap_object(void *obj);
    static herr_t          _free_wrap_ctx(void *obj);
    virtual herr_t          free_wrap_ctx(void *obj);

    // attribute
    static void*           _attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
    virtual void*           attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
    static void*           _attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
    virtual void*           attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
    static herr_t          _attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
    virtual herr_t          attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
    static herr_t          _attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
    virtual herr_t          attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
    static herr_t          _attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _attr_close(void *attr, hid_t dxpl_id, void **req);
    virtual herr_t          attr_close(void *attr, hid_t dxpl_id, void **req);

    // dataset
    static void*           _dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    virtual void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
    static void*           _dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
    virtual void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
    static herr_t          _dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
    virtual herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
    static herr_t          _dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
    virtual herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
    static herr_t          _dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _dataset_specific(void *obj, H5VL_dataset_specific_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          dataset_specific(void *obj, H5VL_dataset_specific_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _dataset_optional(void *obj, H5VL_dataset_optional_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          dataset_optional(void *obj, H5VL_dataset_optional_t get_type, hid_t dxpl_id, void **req, va_list arguments);
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
    static herr_t          _file_specific_reissue(void *obj, hid_t connector_id, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...);   // helper function for _file_specific()
    static herr_t          _file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _file_close(void *file, hid_t dxpl_id, void **req);
    virtual herr_t          file_close(void *file, hid_t dxpl_id, void **req);

    //// group
    static void*           _group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    virtual void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
    static void*           _group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
    virtual void*           group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
    //void group_get()                {}
    //void group_specific()           {}
    //void group_optional()           {}
    static herr_t          _group_close(void *grp, hid_t dxpl_id, void **req);
    virtual herr_t          group_close(void *grp, hid_t dxpl_id, void **req);

    //// link
    static herr_t          _link_create_reissue(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...); // helper function for link_create()
    static herr_t          _link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
    virtual herr_t          link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
    static herr_t          _link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
    virtual herr_t          link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
    static herr_t          _link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          link_get(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          link_specific(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _link_optional(void *obj, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          link_optional(void *obj, hid_t under_vol_id, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments);

    //// object
    static void *          _object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
    virtual void *          object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
    static herr_t          _object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
    virtual herr_t          object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
    static herr_t          _object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
    static herr_t          _object_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments);
    virtual herr_t          object_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments);

    //// Container/connector introspection
    static herr_t          _introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
    virtual herr_t          introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls);
    static herr_t          _introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);
    virtual herr_t          introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported);

    //// blob
    static herr_t          _blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
    virtual herr_t          blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
    static herr_t          _blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
    virtual herr_t          blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
    static herr_t          _blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments);
    virtual herr_t          blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments);
    //void blob_optional              {}

    //// token
    static herr_t          _token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value);
    virtual herr_t          token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value);
    //void token_to_str               {}
    //void token_from_str             {}

    //// request
    //void req_wait()                 {}
    //void req_notify()               {}
    //void req_cancel()               {}
    //void req_specific()             {}
    //void req_optional()             {}
    //void req_free()                 {}
};

}

extern "C"
{
/* Required shim routines, to enable dynamic loading of shared library */
/* The HDF5 library _must_ find routines with these names and signatures
 *      for a shared library that contains a VOL connector to be detected
 *      and loaded at runtime.
 */
H5PL_type_t H5PLget_plugin_type(void);
const void *H5PLget_plugin_info(void);
}

#endif
