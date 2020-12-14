#include <lowfive/vol-base.hpp>

/*---------------------------------------------------------------------------
 * Function:    info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_info_copy(const void *_info)
{
    const info_t *info = (const info_t *)_info;
    return info->vol->info_copy(_info);
}

void *
LowFive::VOLBase::
info_copy(const void *_info)
{
    const info_t *info = (const info_t *)_info;
    info_t *new_info;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the pass through connector */
    new_info = (info_t *)calloc(1, sizeof(info_t));
    new_info->vol = info->vol;

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if(info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return new_info;
} /* end info_copy() */

/*---------------------------------------------------------------------------
 * Function:    info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:        Take care to preserve the current HDF5 error stack
 *              when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_info_free(void *_info)
{
    info_t *info = (info_t *)_info;
    return info->vol->info_free(_info);
}

herr_t
LowFive::VOLBase::
info_free(void *_info)
{
    info_t *info = (info_t *)_info;
    hid_t err_id;

#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through info object itself */
    free(info);

    return 0;
} /* end info_free() */
