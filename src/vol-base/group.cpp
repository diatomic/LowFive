#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
    hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *group;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL GROUP Create");

    under = o->vol->group_create(o->under_object, loc_params, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);
    if(under) {
        group = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        group = NULL;

    return (void *)group;
} /* end group_create() */

void*
LowFive::VOLBase::
group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
    hid_t dxpl_id, void **req)
{
    return H5VLgroup_create(obj, loc_params, info->under_vol_id, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_group_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *group;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL GROUP Open");

    under = o->vol->group_open(o->under_object, loc_params, name, gapl_id, dxpl_id, req);
    if(under) {
        group = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        group = NULL;

    return (void *)group;
} /* end group_open() */

void *
LowFive::VOLBase::
group_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    return H5VLgroup_open(obj, loc_params, info->under_vol_id, name, gapl_id, dxpl_id, req);
}

 /*-------------------------------------------------------------------------
 * Function:    group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_group_close(void *grp, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)grp;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL GROUP close");

    ret_value = o->vol->group_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end group_close() */

herr_t
LowFive::VOLBase::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    return H5VLgroup_close(grp, info->under_vol_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_group_optional(void *obj, H5VL_group_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL GROUP Optional");

    ret_value = o->vol->group_optional(o->under_object, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end group_optional() */

herr_t
LowFive::VOLBase::
group_optional(void *obj, H5VL_group_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLgroup_optional(obj, info->under_vol_id, opt_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    group_get
 *
 * Purpose:     Gets information about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_group_get(void *ob, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)ob;
    herr_t ret_value;

    va_list args;
    va_copy(args,arguments);

    log->debug("------- PASS THROUGH VOL GROUP Get");

    ret_value = o->vol->group_get(o->under_object, get_type, dxpl_id, req, arguments);

    if (get_type == H5VL_GROUP_GET_INFO)
    {
        const H5VL_loc_params_t *loc_params = va_arg(args, const H5VL_loc_params_t *);
        H5G_info_t *             group_info = va_arg(args, H5G_info_t *);

        log->trace("storage_type = {}, nlinks = {}, max_corder = {}, mounted = {}",
                    group_info->storage_type,
                    group_info->nlinks,
                    group_info->max_corder,
                    group_info->mounted);
    }

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end group_get() */

herr_t
LowFive::VOLBase::
group_get(void *dset, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLgroup_get(dset, info->under_vol_id, get_type, dxpl_id, req, arguments);
}

/*-------------------------------------------------------------------------
 * Function:    group_specific
 *
 * Purpose:     Specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_group_specific(void *obj, H5VL_group_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    //hid_t under_vol_id;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL GROUP Specific");

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    //under_vol_id = o->under_vol_id;

    ret_value = o->vol->group_specific(o->under_object, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end group_specific() */

herr_t
LowFive::VOLBase::
group_specific(void *obj, H5VL_group_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return H5VLgroup_specific(obj, info->under_vol_id, specific_type, dxpl_id, req, arguments);
}

