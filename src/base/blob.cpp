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
_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Specific\n");
#endif

    ret_value = o->vol->blob_specific(o->under_object, blob_id, specific_type, arguments);

    return ret_value;
}

herr_t
LowFive::VOLBase::
blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments)
{
    return H5VLblob_specific(obj, info->under_vol_id, blob_id, specific_type, arguments);
}


herr_t
LowFive::VOLBase::
_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef LOWFIVE_ENABLE_EXT_PASSTHRU_LOGGING
    printf("------- EXT PASS THROUGH VOL BLOB Get\n");
#endif

    ret_value = o->vol->blob_get(o->under_object, blob_id, buf, size, ctx);

    return ret_value;
} /* end H5VL_pass_through_ext_blob_get() */

herr_t
LowFive::VOLBase::
blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    return H5VLblob_get(obj, info->under_vol_id, blob_id, buf, size, ctx);
}
