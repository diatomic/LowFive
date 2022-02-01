#include <lowfive/vol-base.hpp>

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
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    fprintf(stderr, "------- PASS THROUGH VOL OBJECT Get\n");
#endif

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
    pass_through_t *o = (pass_through_t *)obj;
    hid_t under_vol_id;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    fprintf(stderr, "------- PASS THROUGH VOL OBJECT Specific\n");
#endif

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

