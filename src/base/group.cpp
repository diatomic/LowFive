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
