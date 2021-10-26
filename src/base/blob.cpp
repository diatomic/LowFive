#include <lowfive/vol-base.hpp>

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
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL BLOB Put\n");
#endif

    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size,
        blob_id, ctx);

    return ret_value;
} /* end _blob_put() */

