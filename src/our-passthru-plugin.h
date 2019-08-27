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

/* Pass-through VOL connector info */
typedef struct OUR_pass_through_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
} OUR_pass_through_info_t;


#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t OUR_pass_through_register(void);

/* added by Tom */
/* C wrappers around custom VOL objects and methods */

void *vol_new();
void  vol_delete(void *vol);
void  vol_init(void *vol);
void  vol_term(void *vol);

void info_copy(void* vol);
void info_cmp(void* vol);
void info_free(void* vol);
void info_to_str(void* vol);
void info_str_to_info(void* vol);

void wrap_get_object(void* vol);
void wrap_get_wrap_ctx(void* vol);
void wrap_wrap_object(void* vol);
void wrap_unwrap_object(void* vol);
void wrap_free_wrap_ctx(void* vol);

void attribute_create(void* vol);
void attribute_open(void* vol);
void attribute_read(void* vol);
void attribute_write(void* vol);
void attribute_get(void* vol);
void attribute_specific(void* vol);
void attribute_optional(void* vol);
void attribute_close(void* vol);

void dataset_create(void* vol);
void dataset_open(void* vol);
void dataset_read(void* vol);
void dataset_write(void* vol);
void dataset_get(void* vol);
void dataset_specific(void* vol);
void dataset_optional(void* vol);
void dataset_close(void* vol);

void datatype_commit(void* vol);
void datatype_open(void* vol);
void datatype_get(void* vol);
void datatype_specific(void* vol);
void datatype_optional(void* vol);
void datatype_close(void* vol);

void file_create(void* vol);
void file_open(void* vol);
void file_get(void* vol);
void file_specific(void* vol);
void file_optional(void* vol);
void file_close(void* vol);

void group_create(void* vol);
void group_open(void* vol);
void group_get(void* vol);
void group_specific(void* vol);
void group_optional(void* vol);
void group_close(void* vol);

void link_create(void* vol);
void link_copy(void* vol);
void link_move(void* vol);
void link_get(void* vol);
void link_specific(void* vol);
void link_optional(void* vol);

void object_open(void* vol);
void object_copy(void* vol);
void object_get(void* vol);
void object_specific(void* vol);
void object_optional(void* vol);

void request_wait(void* vol);
void request_notify(void* vol);
void request_cancel(void* vol);
void request_specific(void* vol);
void request_optional(void* vol);
void request_free(void* vol);

#ifdef __cplusplus
}
#endif

#endif /* _H5VLpassthru_H */

