#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL INTROSPECT GetConnCls");

    /* Check for querying this connector's class */
    if(H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &(o->vol->connector);
        ret_value = 0;
    } /* end if */
    else
        ret_value = o->vol->introspect_get_conn_cls(o->under_object, lvl, conn_cls);

    return ret_value;
} /* end introspect_get_conn_cls() */

herr_t
LowFive::VOLBase::
introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    return H5VLintrospect_get_conn_cls(obj, info->under_vol_id, lvl, conn_cls);
}

herr_t
LowFive::VOLBase::
introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported, uint64_t *flags)
{
#if (H5_VERS_MINOR == 12)
    return H5VLintrospect_opt_query(obj, info->under_vol_id, cls, opt_type, supported);
#elif (H5_VERS_MINOR == 14)
    return H5VLintrospect_opt_query(obj, info->under_vol_id, cls, opt_type, flags);
#endif
}


#if (H5_VERS_MINOR == 12)
/*-------------------------------------------------------------------------
 * Function:    introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL INTROSPECT OptQuery");

    ret_value = o->vol->introspect_opt_query(o->under_object, cls, opt_type, supported);

    return ret_value;
} /* end introspect_opt_query() */

herr_t
LowFive::VOLBase::
introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported)
{
    return H5VLintrospect_opt_query(obj, info->under_vol_id, cls, opt_type, supported);
}

#elif (H5_VERS_MINOR == 14)

/*-------------------------------------------------------------------------
 * Function:    introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL INTROSPECT OptQuery");

    // for backward compatibility with HDF5 1.12
    hbool_t* supported = nullptr;

    ret_value = o->vol->introspect_opt_query(o->under_object, cls, opt_type, supported, flags);

    return ret_value;
} /* end introspect_opt_query() */


/*-------------------------------------------------------------------------
 * Function:    introspect_get_cap_flags
 *
 * Purpose:     Query capability flags.
 *
 * Return:      SUCCEED
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_introspect_get_cap_flags(const void *info, uint64_t *cap_flags)
{
    auto log = get_logger();
    log->trace("------- PASS THROUGH VOL INTROSPECT GetConnCls");

    *cap_flags = _cap_flags;
    return 0;
} /* end introspect_get_cap_flags() */

#endif

