#include <cassert>
#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include "../log-private.hpp"



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
_link_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params,
        hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    auto* destination = static_cast<pass_through_t*>(obj);
    auto* target = args->op_type == H5VL_LINK_CREATE_HARD
        ? static_cast<pass_through_t*>(args->args.hard.curr_obj) : nullptr;
    auto* owner = destination ? destination : target;
    if (!owner)
        return -1;
    auto underlying_args = *args;
    if (target)
        underlying_args.args.hard.curr_obj = target->under_object;
    const herr_t result = owner->vol->link_create(&underlying_args,
        destination ? destination->under_object : nullptr, loc_params,
        owner->under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    if (req && *req)
        *req = owner->create(*req);
    return result;
} /* end pass_through_link_create() */

herr_t
LowFive::VOLBase::link_create(H5VL_link_create_args_t* args, void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req)
{
    return H5VLlink_create(args, obj, loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
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
    auto* source = static_cast<pass_through_t*>(src_obj);
    auto* destination = static_cast<pass_through_t*>(dst_obj);
    auto* owner = source ? source : destination;
    if (!owner)
        return -1;
    const herr_t result = owner->vol->link_copy(source ? source->under_object : nullptr,
        loc_params1, destination ? destination->under_object : nullptr, loc_params2,
        owner->under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    if (req && *req)
        *req = owner->create(*req);
    return result;
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
    auto* source = static_cast<pass_through_t*>(src_obj);
    auto* destination = static_cast<pass_through_t*>(dst_obj);
    auto* owner = source ? source : destination;
    if (!owner)
        return -1;
    const herr_t result = owner->vol->link_move(source ? source->under_object : nullptr,
        loc_params1, destination ? destination->under_object : nullptr, loc_params2,
        owner->under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    if (req && *req)
        *req = owner->create(*req);
    return result;
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
LowFive::VOLBase::_link_get(void* obj, const H5VL_loc_params_t* loc_params, H5VL_link_get_args_t* args, hid_t dxpl_id, void** req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Get");

    ret_value = o->vol->link_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_get() */

herr_t
LowFive::VOLBase::link_get(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_link_get_args_t* args, hid_t dxpl_id, void** req)
{
    return H5VLlink_get(obj, loc_params, under_vol_id, args, dxpl_id, req);
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
LowFive::VOLBase::_link_specific(void* obj, const H5VL_loc_params_t* loc_params,
        H5VL_link_specific_args_t* args, hid_t dxpl_id, void** req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Specific");

    ret_value = o->vol->link_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_specific() */

herr_t
LowFive::VOLBase::link_specific(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id,
        H5VL_link_specific_args_t* args, hid_t dxpl_id, void** req)
{
    return H5VLlink_specific(obj, loc_params, under_vol_id, args, dxpl_id, req);
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
_link_optional(void* obj, const H5VL_loc_params_t* loc_params,
        H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL LINK Optional");

    ret_value = o->vol->link_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);     // TODO: is this right, or should be new pass_through_t(...)

    return ret_value;
} /* end pass_through_link_optional() */

herr_t
LowFive::VOLBase::link_optional(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id,
        H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    return H5VLlink_optional(obj, loc_params, under_vol_id, args, dxpl_id, req);
}