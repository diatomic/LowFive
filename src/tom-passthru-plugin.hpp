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

struct OUR_pass_through_info_t
{
    hid_t   under_vol_id;           // VOL ID for under VOL
    void*   under_vol_info;         // VOL info for under VOL
    void*   vol_derived;            // pointer to custom plugin object TODO: not being used currently
};

/* The connector identification number, initialized at runtime */
static hid_t OUR_PASSTHRU_g = H5I_INVALID_HID;

template<class Vol>
H5VL_class_t create_vl_class();

#include "proto.hpp"
#include "tom-passthru-plugin.cpp"

template<class Vol>
H5VL_class_t create_vl_class()
{
    H5VL_class_t x =
    {
        OUR_PASSTHRU_VERSION,                           /* version      */
        (H5VL_class_value_t)OUR_PASSTHRU_VALUE,         /* value        */
        OUR_PASSTHRU_NAME,                              /* name         */
        0,                                              /* capability flags */
        &OUR_pass_through_init<Vol>,                     /* initialize   */
        &OUR_pass_through_term<Vol>,                     /* terminate    */
        {                                           /* info_cls */
            sizeof(OUR_pass_through_info_t),            /* size    */
            &OUR_pass_through_info_copy,                 /* copy    */
            &OUR_pass_through_info_cmp,                  /* compare */
            &OUR_pass_through_info_free,                 /* free    */
            &OUR_pass_through_info_to_str,               /* to_str  */
            &OUR_pass_through_str_to_info,               /* from_str */
        },
        {                                           /* wrap_cls */
            OUR_pass_through_get_object,               /* get_object   */
            OUR_pass_through_get_wrap_ctx,             /* get_wrap_ctx */
            OUR_pass_through_wrap_object,              /* wrap_object  */
            OUR_pass_through_unwrap_object,            /* unwrap_object */
            OUR_pass_through_free_wrap_ctx,            /* free_wrap_ctx */
        },
        {                                           /* attribute_cls */
            OUR_pass_through_attr_create,              /* create */
            OUR_pass_through_attr_open,                /* open */
            OUR_pass_through_attr_read,                /* read */
            OUR_pass_through_attr_write,               /* write */
            OUR_pass_through_attr_get,                 /* get */
            OUR_pass_through_attr_specific,            /* specific */
            OUR_pass_through_attr_optional,            /* optional */
            OUR_pass_through_attr_close                /* close */
        },
        {                                           /* dataset_cls */
            OUR_pass_through_dataset_create,           /* create */
            OUR_pass_through_dataset_open,             /* open */
            OUR_pass_through_dataset_read,             /* read */
            OUR_pass_through_dataset_write,            /* write */
            OUR_pass_through_dataset_get,              /* get */
            OUR_pass_through_dataset_specific,         /* specific */
            OUR_pass_through_dataset_optional,         /* optional */
            OUR_pass_through_dataset_close             /* close */
        },
        {                                           /* datatype_cls */
            OUR_pass_through_datatype_commit,          /* commit */
            OUR_pass_through_datatype_open,            /* open */
            OUR_pass_through_datatype_get,             /* get_size */
            OUR_pass_through_datatype_specific,        /* specific */
            OUR_pass_through_datatype_optional,        /* optional */
            OUR_pass_through_datatype_close            /* close */
        },
        {                                           /* file_cls */
            OUR_pass_through_file_create,              /* create */
            OUR_pass_through_file_open,                /* open */
            OUR_pass_through_file_get,                 /* get */
            OUR_pass_through_file_specific,            /* specific */
            OUR_pass_through_file_optional,            /* optional */
            OUR_pass_through_file_close                /* close */
        },
        {                                           /* group_cls */
            OUR_pass_through_group_create,             /* create */
            OUR_pass_through_group_open,               /* open */
            OUR_pass_through_group_get,                /* get */
            OUR_pass_through_group_specific,           /* specific */
            OUR_pass_through_group_optional,           /* optional */
            OUR_pass_through_group_close               /* close */
        },
        {                                           /* link_cls */
            OUR_pass_through_link_create,              /* create */
            OUR_pass_through_link_copy,                /* copy */
            OUR_pass_through_link_move,                /* move */
            OUR_pass_through_link_get,                 /* get */
            OUR_pass_through_link_specific,            /* specific */
            OUR_pass_through_link_optional,            /* optional */
        },
        {                                           /* object_cls */
            OUR_pass_through_object_open,              /* open */
            OUR_pass_through_object_copy,              /* copy */
            OUR_pass_through_object_get,               /* get */
            OUR_pass_through_object_specific,          /* specific */
            OUR_pass_through_object_optional,          /* optional */
        },
        {                                           /* request_cls */
            OUR_pass_through_request_wait,             /* wait */
            OUR_pass_through_request_notify,           /* notify */
            OUR_pass_through_request_cancel,           /* cancel */
            OUR_pass_through_request_specific,         /* specific */
            OUR_pass_through_request_optional,         /* optional */
            OUR_pass_through_request_free              /* free */
        },
        NULL                                        /* optional */
    };
    //OUR_pass_through_g = x;
    return x;
}

//#ifdef __cplusplus
//extern "C" {
//#endif

//H5_DLL hid_t OUR_pass_through_register(void);

//#ifdef __cplusplus
//}
//#endif

#endif /* _H5VLpassthru_H */

