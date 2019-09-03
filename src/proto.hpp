/* Public HDF5 file */
#include "hdf5.h"

// The pass through VOL info object
struct OUR_pass_through_t
{
    hid_t   under_vol_id;           // ID for underlying VOL connector
    void*   under_object;           // Info object for underlying VOL connector */
    void*   vol_derived;            // pointer to custom plugin object TODO: not being used currently
};

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */
static herr_t OUR_pass_through_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...);
static herr_t OUR_pass_through_request_specific_reissue(void *obj, hid_t connector_id,
    H5VL_request_specific_t specific_type, ...);
static herr_t OUR_pass_through_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...);
static OUR_pass_through_t *OUR_pass_through_new_obj(void *under_obj,
    hid_t under_vol_id);
static herr_t OUR_pass_through_free_obj(OUR_pass_through_t *obj);

/* "Management" callbacks */
//template <typename V>
//herr_t OUR_pass_through_init(hid_t vipl_id);
//template <typename V>
//herr_t OUR_pass_through_term(void);

/*-------------------------------------------------------------------------
 * Function:    OUR_pass_through_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<typename V>
herr_t
OUR_pass_through_init(hid_t vipl_id)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INIT\n");
#endif

    // added by Tom
    V vol;
    vol.init();

    // Shut compiler up about unused parameter
    vipl_id = vipl_id;

    return 0;
}

/*---------------------------------------------------------------------------
 * Function:    OUR_pass_through_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
template <typename V>
herr_t
OUR_pass_through_term(void)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL TERM\n");
#endif

    // added by Tom
    V vol;
    vol.term();

    // Reset VOL ID
    OUR_PASSTHRU_g = H5I_INVALID_HID;

    return 0;
}


#ifdef __cplusplus
extern "C" {
#endif


// TODO: templatize remaining functions

/* VOL info callbacks */
static void *OUR_pass_through_info_copy(const void *info);
static herr_t OUR_pass_through_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t OUR_pass_through_info_free(void *info);
static herr_t OUR_pass_through_info_to_str(const void *info, char **str);
static herr_t OUR_pass_through_str_to_info(const char *str, void **info);

/* VOL object wrap / retrieval callbacks */
static void *OUR_pass_through_get_object(const void *obj);
static herr_t OUR_pass_through_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void *OUR_pass_through_wrap_object(void *obj, H5I_type_t obj_type,
    void *wrap_ctx);
static void *OUR_pass_through_unwrap_object(void *obj);
static herr_t OUR_pass_through_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void *OUR_pass_through_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
static void *OUR_pass_through_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_attr_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_attr_close(void *attr, hid_t dxpl_id, void **req); 

/* Dataset callbacks */
static void *OUR_pass_through_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *OUR_pass_through_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                    hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t OUR_pass_through_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t OUR_pass_through_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_dataset_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void *OUR_pass_through_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *OUR_pass_through_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_datatype_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void *OUR_pass_through_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *OUR_pass_through_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_file_optional(void *file, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *OUR_pass_through_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *OUR_pass_through_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_group_specific(void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_group_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t OUR_pass_through_link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_link_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);

/* Object callbacks */
static void *OUR_pass_through_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t OUR_pass_through_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t OUR_pass_through_object_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);

/* Async request callbacks */
static herr_t OUR_pass_through_request_wait(void *req, uint64_t timeout, H5ES_status_t *status);
static herr_t OUR_pass_through_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t OUR_pass_through_request_cancel(void *req);
static herr_t OUR_pass_through_request_specific(void *req, H5VL_request_specific_t specific_type, va_list arguments);
static herr_t OUR_pass_through_request_optional(void *req, va_list arguments);
static herr_t OUR_pass_through_request_free(void *req);

#ifdef __cplusplus
}
#endif

