#include <lowfive/vol-base.hpp>
#include <assert.h>
#include "../log-private.hpp"

/*---------------------------------------------------------------------------
 * Function:    _token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL TOKEN Compare");

    /* Sanity checks */
    assert(obj);
    assert(token1);
    assert(token2);
    assert(cmp_value);

    ret_value = o->vol->token_cmp(o->under_object, token1, token2, cmp_value);

    return ret_value;
} /* end _token_cmp() */

herr_t
LowFive::VOLBase::
token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    return H5VLtoken_cmp(obj, info->under_vol_id, token1, token2, cmp_value);
}
