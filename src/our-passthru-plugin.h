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

#ifdef __cplusplus
}
#endif

#endif /* _H5VLpassthru_H */

