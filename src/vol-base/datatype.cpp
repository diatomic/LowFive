#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *dt;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL DATATYPE Commit");

    under = o->vol->datatype_commit(o->under_object, loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);
    if(under) {
        dt = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end datatype_commit() */

void *
LowFive::VOLBase::
datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    return H5VLdatatype_commit(obj, loc_params, info->under_vol_id, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *dt;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL DATATYPE Open");

    under = o->vol->datatype_open(o->under_object, loc_params, name, tapl_id, dxpl_id, req);
    if(under) {
        dt = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end datatype_open() */

void *
LowFive::VOLBase::
datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    return H5VLdatatype_open(obj, loc_params, info->under_vol_id, name, tapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_datatype_get(void *dt, H5VL_datatype_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)dt;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATATYPE Get");

    ret_value = o->vol->datatype_get(o->under_object, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end datatype_get() */

herr_t
LowFive::VOLBase::
datatype_get(void *dt, H5VL_datatype_get_t get_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLdatatype_get(dt, info->under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_datatype_specific(void *obj, H5VL_datatype_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATATYPE Specific");

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = o->vol->datatype_specific(o->under_object, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end datatype_specific() */

herr_t
LowFive::VOLBase::
datatype_specific(void *obj, H5VL_datatype_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLdatatype_specific(obj, info->under_vol_id, specific_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_datatype_optional(void *obj, H5VL_datatype_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATATYPE Optional");

    ret_value = o->vol->datatype_optional(o->under_object, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end datatype_optional() */

herr_t
LowFive::VOLBase::
datatype_optional(void *obj, H5VL_datatype_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLdatatype_optional(obj, info->under_vol_id, opt_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)dt;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATATYPE Close");

    assert(o->under_object);

    ret_value = o->vol->datatype_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying datatype was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end datatype_close() */

herr_t
LowFive::VOLBase::
datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    return H5VLdatatype_close(dt, info->under_vol_id, dxpl_id, req);
}

