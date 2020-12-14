#include <lowfive/vol-base.hpp>

/*-------------------------------------------------------------------------
 * Function:    file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Create\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = info->vol->file_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    if(under) {
        file = new pass_through_t(under, info->under_vol_id, info->vol);

        /* Check for async request */
        if(req && *req)
            *req = new pass_through_t(*req, info->under_vol_id, info->vol);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    _info_free(info);

    return (void *)file;
} /* end file_create() */

void*
LowFive::VOLBase::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    return H5VLfile_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = info->vol->file_open(name, flags, under_fapl_id, dxpl_id, req);
    if(under) {
        file = new pass_through_t(under, info->under_vol_id, info->vol);

        /* Check for async request */
        if(req && *req)
            *req = new pass_through_t(*req, info->under_vol_id, info->vol);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    _info_free(info);

    return (void *)file;
} /* end file_open() */

void*
LowFive::VOLBase::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    return H5VLfile_open(name, flags, fapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL FILE Get\n");
#endif

    ret_value = o->vol->file_get(o->under_object, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_get() */

herr_t
LowFive::VOLBase::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    return H5VLfile_get(file, info.under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_optional(void *file, H5VL_file_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL File Optional\n");
#endif

    ret_value = o->vol->file_optional(o->under_object, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_optional() */

herr_t
LowFive::VOLBase::
file_optional(void *file, H5VL_file_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLfile_optional(file, info.under_vol_id, opt_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_close(void *file, hid_t dxpl_id, void **req)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Close\n");
#endif

    ret_value = o->vol->file_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end file_close() */

herr_t
LowFive::VOLBase::
file_close(void *file, hid_t dxpl_id, void **req)
{
    return H5VLfile_close(file, info.under_vol_id, dxpl_id, req);
}
