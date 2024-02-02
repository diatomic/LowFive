#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

/*-------------------------------------------------------------------------
 * Function:    _blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL BLOB Put");

    ret_value = o->vol->blob_put(o->under_object, buf, size, blob_id, ctx);

    return ret_value;
} /* end _blob_put() */

herr_t
LowFive::VOLBase::
blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    return H5VLblob_put(obj, info->under_vol_id, buf, size, blob_id, ctx);
}

herr_t
LowFive::VOLBase::
_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t* args)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- EXT PASS THROUGH VOL BLOB Specific");

    ret_value = o->vol->blob_specific(o->under_object, blob_id, args);

    log->debug("------- EXT PASS THROUGH VOL BLOB Specific EXIT");

    return ret_value;
}

herr_t
LowFive::VOLBase::
blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t* args)
{
    return H5VLblob_specific(obj, info->under_vol_id, blob_id, args);
}

herr_t
LowFive::VOLBase::
_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- EXT PASS THROUGH VOL BLOB Get");

    ret_value = o->vol->blob_get(o->under_object, blob_id, buf, size, ctx);

    log->debug("------- EXT PASS THROUGH VOL BLOB Get EXIT");

    return ret_value;
} /* end H5VL_pass_through_ext_blob_get() */

herr_t
LowFive::VOLBase::
blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    return H5VLblob_get(obj, info->under_vol_id, blob_id, buf, size, ctx);
}
