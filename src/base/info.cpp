#include <lowfive/vol-base.hpp>
#include <string.h>
#include <cassert>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include "../log-private.hpp"

LowFive::VOLBase::info_t* LowFive::VOLBase::info = NULL;
LowFive::VOLBase* LowFive::VOLBase::info_t::vol = NULL;

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
    auto log = get_logger();

    const info_t *info = (const info_t *)_info;
    info_t *new_info;

    log->debug("------- PASS THROUGH VOL INFO Copy");

    log->trace("_info_copy(), info = {}", fmt::ptr(info));

    /* Allocate new VOL info struct for the pass through connector */
    new_info = (info_t *)calloc(1, sizeof(info_t));
    new_info->vol = info->vol;
    new_info->under_vol_info = NULL;

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    log->trace("VOLBase:_info_copy, inc_ref hid = {}", new_info->under_vol_id);
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
    auto log = get_logger();

    info_t *info = (info_t *)_info;
    hid_t err_id;

    log->debug("------- PASS THROUGH VOL INFO Free");

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);
    log->trace("VOLBase::_info_free, dec ref hid = {}", info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through info object itself */
    //if (info != VOLBase::info)
        free(info);

    return 0;
} /* end info_free() */

/*---------------------------------------------------------------------------
 * Function:    info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_info_to_str(const void *_info, char **str)
{
    auto log = get_logger();

    const info_t *info = (const info_t *)_info;
    H5VL_class_value_t under_value = (H5VL_class_value_t)-1;
    char *under_vol_string = NULL;
    size_t under_vol_str_len = 0;

    log->debug("------- PASS THROUGH VOL INFO To String");

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if(under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s}", (unsigned)under_value, (under_vol_string ? under_vol_string : ""));

    return 0;
} /* end info_to_str() */

/*---------------------------------------------------------------------------
 * Function:    str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_str_to_info(const char *str, void **_info)
{
    auto log = get_logger();

    unsigned under_vol_value;
    const char *under_vol_info_start, *under_vol_info_end;
    hid_t under_vol_id;
    void *under_vol_info = NULL;

    log->debug("------- PASS THROUGH VOL INFO String To Info");

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if(under_vol_info_end != (under_vol_info_start + 1)) {
        char *under_vol_info_str;

        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1, (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);

        free(under_vol_info_str);
    } /* end else */

    info = new info_t;

    // set the class static info
    info->under_vol_id = under_vol_id;
    info->under_vol_info = under_vol_info;
    // NB: info->vol not set because we don't have it;
    //     it needs to be set before the info starts being copied around

    /* Set return value */
    *_info = info;

    log->trace("Returning, NB: info->vol not yet set");

    return 0;
} /* end str_to_info() */
