// Copied from User Guide for Developing a Virtual Object Layer Plugin
// Mohamad Chaarawi
// 1The HDF Group, hdfgroup.org
// 12th January 2018

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "hdf5.h"

#define LOG 502

static herr_t H5VL_log_init(hid_t vipl_id);
static herr_t H5VL_log_term(hid_t vtpl_id);

// Datatype callbacks
static void *H5VL_log_datatype_commit(void *obj, H5VL_loc_params_t loc_params, const char *name,
        hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_log_datatype_open(void *obj, H5VL_loc_params_t loc_params, const char *name,
        hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_log_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void
        **req, va_list arguments);
static herr_t H5VL_log_datatype_close(void *dt, hid_t dxpl_id, void **req);

// Dataset callbacks
static void *H5VL_log_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name,
        hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_log_dataset_open(void *obj, H5VL_loc_params_t loc_params, const char *name,
        hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_log_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
        hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_log_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
        hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_log_dataset_close(void *dset, hid_t dxpl_id, void **req);

// File callbacks
static void *H5VL_log_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
        hid_t dxpl_id, void **req);
static void *H5VL_log_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id,
        void **req);
static herr_t H5VL_log_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req,
        va_list arguments);
static herr_t H5VL_log_file_close(void *file, hid_t dxpl_id, void **req);

// Group callbacks
static void *H5VL_log_group_create(void *obj, H5VL_loc_params_t loc_params, const char *name,
        hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_log_group_close(void *grp, hid_t dxpl_id, void **req);

// Link callbacks

// Object callbacks
static void *H5VL_log_object_open(void *obj, H5VL_loc_params_t loc_params, H5I_type_t
        *opened_type, hid_t dxpl_id, void **req);
static herr_t H5VL_log_object_specific(void *obj, H5VL_loc_params_t loc_params,
        H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);


static herr_t visit_cb(hid_t oid, const char *name, const H5O_info_t *oinfo, void *udata);
