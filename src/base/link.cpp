#include <cassert>
#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_create_reissue
 *
 * Purpose:     Re-wrap vararg arguments into a va_list and reissue the
 *              link create callback to the underlying VOL connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_create_reissue(H5VL_link_create_type_t create_type,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t connector_id,
    hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, ...)
{
    auto log = get_logger();

    // TODO: is this right? making a new object from the reissued one?
    pass_through_t *o = (pass_through_t *)obj;

    log->debug("------- PASS THROUGH VOL LINK Create Reissue");

    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    // TODO: is this right? making a new object from the reissued one?
    ret_value = o->vol->link_create(create_type, o->under_object, loc_params, connector_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end pass_through_link_create_reissue() */

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_create(H5VL_link_create_type_t create_type, void *obj,
    const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Create");

    /* Try to retrieve the "under" VOL id */
    if(o)
        under_vol_id = o->under_vol_id;

    /* Fix up the link target object for hard link creation */
    if(H5VL_LINK_CREATE_HARD == create_type) {
        void         *cur_obj;
        H5VL_loc_params_t* cur_params;

        /* Retrieve the object & loc params for the link target */
        cur_obj = va_arg(arguments, void *);
        cur_params = va_arg(arguments, H5VL_loc_params_t*);

        /* If it's a non-NULL pointer, find the 'under object' and re-set the property */
        if(cur_obj) {
            /* Check if we still need the "under" VOL ID */
            if(under_vol_id < 0)
                under_vol_id = ((pass_through_t *)cur_obj)->under_vol_id;

            /* Set the object for the link target */
            cur_obj = ((pass_through_t *)cur_obj)->under_object;
        } /* end if */

        /* Re-issue 'link create' call, using the unwrapped pieces */
        ret_value = _link_create_reissue(create_type, obj, loc_params,
                under_vol_id, lcpl_id, lapl_id, dxpl_id, req, cur_obj, cur_params);
    } /* end if */
    else
        ret_value = o->vol->link_create(create_type, o->under_object, loc_params,
                under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_create() */

herr_t
LowFive::VOLBase::
link_create(H5VL_link_create_type_t create_type, void *obj,
    const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLlink_create(create_type, obj, loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o_src = (pass_through_t *)src_obj;
    pass_through_t *o_dst = (pass_through_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Copy");

    /* Retrieve the "under" VOL id */
    if(o_src)
    {
        under_vol_id = o_src->under_vol_id;
        ret_value = o_src->vol->link_copy(o_src->under_object, loc_params1,
                o_dst->under_object, loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        /* Check for async request */
        if(req && *req)
            *req = o_src->create(*req);     // TODO: is this right, or should be new pass_through_t(...)
    }
    else if(o_dst)
    {
        under_vol_id = o_dst->under_vol_id;
        ret_value = o_dst->vol->link_copy(o_src->under_object, loc_params1,
                o_dst->under_object, loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        /* Check for async request */
        if(req && *req)
            *req = o_dst->create(*req);     // TODO: is this right, or should be new pass_through_t(...)
    }
    assert(under_vol_id > 0);


    return ret_value;
} /* end pass_through_link_copy() */

herr_t
LowFive::VOLBase::
link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    return H5VLlink_copy(src_obj, loc_params1, dst_obj, loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o_src = (pass_through_t *)src_obj;
    pass_through_t *o_dst = (pass_through_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Move");

    /* Retrieve the "under" VOL id */
    if(o_src)
    {
        under_vol_id = o_src->under_vol_id;
        ret_value = o_src->vol->link_move(o_src->under_object, loc_params1, o_dst->under_object,
                loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        /* Check for async request */
        if(req && *req)
            *req = o_src->create(*req);     // TODO: is this right, or should be new pass_through_t(...)
    }
    else if(o_dst)
    {
        under_vol_id = o_dst->under_vol_id;
        ret_value = o_dst->vol->link_move(o_src->under_object, loc_params1, o_dst->under_object,
                loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        /* Check for async request */
        if(req && *req)
            *req = o_dst->create(*req);     // TODO: is this right, or should be new pass_through_t(...)
    }
    assert(under_vol_id > 0);

    return ret_value;
} /* end pass_through_link_move() */

herr_t
LowFive::VOLBase::
link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    return H5VLlink_move(src_obj, loc_params1, dst_obj, loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_get(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Get");

    ret_value = o->vol->link_get(o->under_object, loc_params, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_get() */

herr_t
LowFive::VOLBase::
link_get(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
    H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLlink_get(obj, loc_params, under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_specific(void *obj, const H5VL_loc_params_t *loc_params, 
    H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Specific");

    ret_value = o->vol->link_specific(o->under_object, loc_params, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_specific() */

herr_t
LowFive::VOLBase::
link_specific(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
    H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLlink_specific(obj, loc_params, under_vol_id, specific_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    pass_through_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_link_optional(void *obj, H5VL_link_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Optional");

    ret_value = o->vol->link_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_optional() */

herr_t
LowFive::VOLBase::
link_optional(void *obj, hid_t under_vol_id, H5VL_link_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLlink_optional(obj, under_vol_id, opt_type, dxpl_id, req, arguments);
}


