#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *attr;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Create");

    under = o->vol->attr_create(o->under_object, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);

    if(under) {
        attr = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        attr = NULL;

    return (void*)attr;
} /* end attr_create() */

void*
LowFive::VOLBase::
attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req)
{
    return H5VLattr_create(obj, loc_params, info->under_vol_id, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_attr_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *attr;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Open");

    under = o->vol->attr_open(o->under_object, loc_params, name, aapl_id, dxpl_id, req);
    if(under) {
        attr = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        attr = NULL;

    return (void *)attr;
} /* end attr_open() */

void*
LowFive::VOLBase::
attr_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    return H5VLattr_open(obj, loc_params, info->under_vol_id, name, aapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)attr;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Read");

    ret_value = o->vol->attr_read(o->under_object, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end attr_read() */

herr_t
LowFive::VOLBase::
attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    return H5VLattr_read(attr, info->under_vol_id, mem_type_id, buf, dxpl_id, req);
}


/*-------------------------------------------------------------------------
 * Function:    attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_write(void *attr, hid_t mem_type_id, const void *buf,
    hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)attr;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Write");

    ret_value = o->vol->attr_write(o->under_object, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end attr_write() */

herr_t
LowFive::VOLBase::
attr_write(void *attr, hid_t mem_type_id, const void *buf,
    hid_t dxpl_id, void **req)
{
    return H5VLattr_write(attr, info->under_vol_id, mem_type_id, buf, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Get");

    ret_value = o->vol->attr_get(o->under_object, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end attr_get() */

herr_t
LowFive::VOLBase::
attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    return H5VLattr_get(obj, info->under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Specific");

    ret_value = o->vol->attr_specific(o->under_object, loc_params, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end attr_specific() */

herr_t
LowFive::VOLBase::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLattr_specific(obj, loc_params, info->under_vol_id, specific_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Optional");

    ret_value = o->vol->attr_optional(o->under_object, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end attr_optional() */

herr_t
LowFive::VOLBase::
attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    return H5VLattr_optional(obj, info->under_vol_id, opt_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_attr_close(void *attr, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)attr;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL ATTRIBUTE Close");

    ret_value = o->vol->attr_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying attribute was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end attr_close() */

herr_t
LowFive::VOLBase::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    return H5VLattr_close(attr, info->under_vol_id, dxpl_id, req);
}
