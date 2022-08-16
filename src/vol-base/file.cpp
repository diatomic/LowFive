#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include "../log-private.hpp"

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
    auto log = get_logger();

    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

    log->debug("------- PASS THROUGH VOL FILE Create");

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    log->trace("got vol info: {} with vol {}", fmt::ptr(info), fmt::ptr(info->vol));

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
    auto log = get_logger();

    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

    log->debug("------- PASS THROUGH VOL FILE Open");

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
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Get");

    log->trace("file_get: get_type = {}", get_type);
    va_list args;
    va_copy(args, arguments);

    ret_value = o->vol->file_get(o->under_object, get_type, dxpl_id, req, arguments);

    if (get_type == H5VL_FILE_GET_OBJ_COUNT)
    {
        unsigned types     = va_arg(args, unsigned);
        ssize_t *ret       = va_arg(args, ssize_t *);
        log->trace("file_get: H5VL_FILE_GET_OBJ_COUNT, types = {}, ret = {}", types, *ret);
    }

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
    return H5VLfile_get(file, info->under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_file_specific_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              file specific callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_specific_reissue(void *obj, hid_t connector_id,
    H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...)
{
    // TODO: is this right? making a new object from the reissued one?
    pass_through_t *o = (pass_through_t *)obj;

    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    // TODO: is this right? making a new object from the reissued one?
    ret_value = o->vol->file_specific(o->under_object, specific_type, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end _file_specific_reissue() */

/*-------------------------------------------------------------------------
 * Function:    _file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_specific(void *file, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    hid_t under_vol_id = -1;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Specific");

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if(specific_type == H5VL_FILE_MOUNT) {
        H5I_type_t loc_type;
        const char *name;
        pass_through_t *child_file;
        hid_t plist_id;

        /* Retrieve parameters for 'mount' operation, so we can unwrap the child file */
        loc_type = (H5I_type_t)va_arg(arguments, int); /* enum work-around */
        name = va_arg(arguments, const char *);
        child_file = (pass_through_t *)va_arg(arguments, void *);
        plist_id = va_arg(arguments, hid_t);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        /* Re-issue 'file specific' call, using the unwrapped pieces */
        ret_value = _file_specific_reissue(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, (int)loc_type, name, child_file->under_object, plist_id);
    } /* end if */
    else if(specific_type == H5VL_FILE_IS_ACCESSIBLE || specific_type == H5VL_FILE_DELETE) {
        info_t *info;
        hid_t fapl_id, under_fapl_id;
        const char *name;
        htri_t *ret;

        /* Get the arguments for the 'is accessible' check */
        fapl_id = va_arg(arguments, hid_t);
        name    = va_arg(arguments, const char *);
        ret     = va_arg(arguments, htri_t *);

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(fapl_id, (void **)&info);

        /* Copy the FAPL */
        under_fapl_id = H5Pcopy(fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = info->under_vol_id;

        /* Re-issue 'file specific' call */
        ret_value = _file_specific_reissue(NULL, info->under_vol_id, specific_type, dxpl_id, req, under_fapl_id, name, ret);

        /* Close underlying FAPL */
        H5Pclose(under_fapl_id);

        /* Release copy of our VOL info */
        _info_free(info);
    } /* end else-if */
    else {
        va_list my_arguments;

        /* Make a copy of the argument list for later, if reopening */
        if(specific_type == H5VL_FILE_REOPEN)
            va_copy(my_arguments, arguments);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        ret_value = o->vol->file_specific(o->under_object, specific_type, dxpl_id, req, arguments);

        /* Wrap file struct pointer, if we reopened one */
        if(specific_type == H5VL_FILE_REOPEN) {
            if(ret_value >= 0) {
                void      **ret = va_arg(my_arguments, void **);

                if(ret && *ret)
                    *ret = o->create(*ret);
            } /* end if */

            /* Finish use of copied vararg list */
            va_end(my_arguments);
        } /* end if */
    } /* end else */

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end _file_specific() */

herr_t
LowFive::VOLBase::
file_specific(void *file, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLfile_specific(file, info->under_vol_id, specific_type, dxpl_id, req, arguments);
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
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL File Optional");

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
    return H5VLfile_optional(file, info->under_vol_id, opt_type, dxpl_id, req, arguments);
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
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Close");

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
    return H5VLfile_close(file, info->under_vol_id, dxpl_id, req);
}
