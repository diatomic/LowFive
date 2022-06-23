#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    _object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_object_open(void *obj, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *new_obj;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL OBJECT Open");

    log->trace("Calling o->vol->object_open()");
    under = o->vol->object_open(o->under_object, loc_params, opened_type, dxpl_id, req);
    log->trace("Got: opened_type = {}", *opened_type);
    if(under) {
        new_obj = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        new_obj = NULL;

    return (void *)new_obj;
} /* end _object_open() */

void *
LowFive::VOLBase::
object_open(void *obj, const H5VL_loc_params_t *loc_params,
    H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("VOLBase::object_open()");
    return H5VLobject_open(obj, loc_params, info->under_vol_id, opened_type, dxpl_id, req);
}
/*-------------------------------------------------------------------------
 * Function:    _object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id,
    void **req)
{
    auto log = get_logger();

    pass_through_t *o_src = (pass_through_t *)src_obj;
    pass_through_t *o_dst = (pass_through_t *)dst_obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL OBJECT Copy");

    // TODO: using o_src->vol->object_copy, not o_dst->vol->object_copy; is this right?
    ret_value = o_src->vol->object_copy(o_src->under_object, src_loc_params, src_name, o_dst->under_object, dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req);

    /* Check for async request */
    // TODO: using o_src->create, not o_dst->create; is this right?
    if(req && *req)
        *req = o_src->create(*req);

    return ret_value;
} /* end _object_copy() */

herr_t
LowFive::VOLBase::
object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id,
    void **req)
{
    return H5VLobject_copy(src_obj, src_loc_params, src_name, dst_obj, dst_loc_params, dst_name, info->under_vol_id, ocpypl_id, lcpl_id, dxpl_id, req);
}
/*-------------------------------------------------------------------------
 * Function:    _object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL OBJECT Get");

    ret_value = o->vol->object_get(o->under_object, loc_params, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end _object_get() */

herr_t
LowFive::VOLBase::
object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLobject_get(obj, loc_params, info->under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    _object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL OBJECT Specific");

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;

    ret_value = o->vol->object_specific(o->under_object, loc_params, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end _object_specific() */

herr_t
LowFive::VOLBase::
object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLobject_specific(obj, loc_params, info->under_vol_id, specific_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    _object_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_object_optional(void *obj, int op_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL generic Optional");

    ret_value = o->vol->object_optional(o->under_object, op_type, dxpl_id, req, arguments);

    return ret_value;
} /* end _object_optional() */


herr_t
LowFive::VOLBase::
object_optional(void *obj, int op_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    return H5VLoptional(obj, info->under_vol_id, op_type, dxpl_id, req, arguments);
}
