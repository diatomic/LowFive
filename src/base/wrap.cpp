#include <lowfive/vol-base.hpp>

/*---------------------------------------------------------------------------
 * Function:    wrap_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_wrap_get_object(const void *obj)
{
    const pass_through_t *o = (const pass_through_t *)obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL Get object\n");
#endif

    return H5VLget_object(o->under_object, o->under_vol_id);

} /* end wrap_get_object() */

/*---------------------------------------------------------------------------
 * Function:    get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    const pass_through_t *o = (const pass_through_t *)obj;
    pass_through_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL WRAP CTX Get\n");
#endif

    /* Allocate new VOL object wrapping context for the pass through connector */
    new_wrap_ctx = (pass_through_wrap_ctx_t *)calloc(1, sizeof(pass_through_wrap_ctx_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    return 0;
} /* end get_wrap_ctx() */

/*---------------------------------------------------------------------------
 * Function:    wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_wrap_object(void *obj, H5I_type_t obj_type, void *_wrap_ctx)
{
    pass_through_t *o = (pass_through_t *)obj;
    pass_through_wrap_ctx_t *wrap_ctx = (pass_through_wrap_ctx_t *)_wrap_ctx;
    pass_through_t *new_obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
    if(under)
        new_obj = o->create(under);
    else
        new_obj = NULL;

    return new_obj;
} /* end wrap_object() */

/*---------------------------------------------------------------------------
 * Function:    unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *
LowFive::VOLBase::
_unwrap_object(void *obj)
{
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if(under)
        pass_through_t::destroy(o);

    return under;
} /* end unwrap_object() */

/*---------------------------------------------------------------------------
 * Function:    free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Note: Take care to preserve the current HDF5 error stack when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_free_wrap_ctx(void *_wrap_ctx)
{
    pass_through_wrap_ctx_t *wrap_ctx = (pass_through_wrap_ctx_t *)_wrap_ctx;
    hid_t err_id;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if(wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through wrap context object itself */
    free(wrap_ctx);

    return 0;
} /* end free_wrap_ctx() */

