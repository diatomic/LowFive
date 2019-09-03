/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	The public header file for the pass-through VOL connector.
 */

#ifndef _OURpassthru_H
#define _OURpassthru_H

/* Identifier for the pass-through VOL connector */
#define OUR_PASSTHRU	(OUR_pass_through_register())

/* Characteristics of the pass-through VOL connector */
#define OUR_PASSTHRU_NAME        "our_pass_through"
#define OUR_PASSTHRU_VALUE       510           /* VOL connector ID */
#define OUR_PASSTHRU_VERSION     0

// Templated version of class information for each VOL connector
template <typename V>
struct H5VL_class
{
    /* Overall connector fields & callbacks */
    unsigned int version;                   /* VOL connector class struct version #     */
    H5VL_class_value_t value;               /* Value to identify connector              */
    const char *name;                       /* Connector name (MUST be unique!)         */
    unsigned cap_flags;                     /* Capability flags for connector           */
    herr_t (*initialize)(hid_t vipl_id);    /* Connector initialization callback        */
    herr_t (*terminate)(void);              /* Connector termination callback           */

    /* VOL framework */
    H5VL_info_class_t       info_cls;       /* VOL info fields & callbacks  */
    H5VL_wrap_class_t       wrap_cls;       /* VOL object wrap / retrieval callbacks */

    /* Data Model */
    H5VL_attr_class_t       attr_cls;       /* Attribute (H5A*) class callbacks */
    H5VL_dataset_class_t    dataset_cls;    /* Dataset (H5D*) class callbacks   */
    H5VL_datatype_class_t   datatype_cls;   /* Datatype (H5T*) class callbacks  */
    H5VL_file_class_t       file_cls;       /* File (H5F*) class callbacks      */
    H5VL_group_class_t      group_cls;      /* Group (H5G*) class callbacks     */
    H5VL_link_class_t       link_cls;       /* Link (H5L*) class callbacks      */
    H5VL_object_class_t     object_cls;     /* Object (H5O*) class callbacks    */

    /* Services */
    H5VL_request_class_t    request_cls;    /* Asynchronous request class callbacks */

    /* Catch-all */
    herr_t (*optional)(void *obj, hid_t dxpl_id, void **req, va_list arguments); /* Optional callback */
};

struct OUR_pass_through_info_t
{
    hid_t   under_vol_id;           // VOL ID for under VOL
    void*   under_vol_info;         // VOL info for under VOL
    void*   vol_derived;            // pointer to custom plugin object TODO: not being used currently
};

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t OUR_pass_through_register(void);

#ifdef __cplusplus
}
#endif

#endif /* _H5VLpassthru_H */

