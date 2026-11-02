#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

#include <cstdlib>

namespace
{
// Roll back unpublished contexts without replacing the originating error.
void discard_context(LowFive::VOLBase::pass_through_wrap_ctx_t* context, bool retained) noexcept
{
    const hid_t stack = H5Eget_current_stack();
    if (context->under_wrap_ctx)
    {
        try { context->vol->free_wrap_ctx(context->under_wrap_ctx); }
        catch (...) {}
    }
    if (retained) H5Idec_ref(context->under_vol_id);
    std::free(context);
    H5Eset_current_stack(stack);
}
}

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
    if (!obj) return nullptr;
    auto log = get_logger();

    const pass_through_t *o = (const pass_through_t *)obj;

    log->debug("------- PASS THROUGH VOL WRAP Get object");

    return o->vol->wrap_get_object(o->under_object);
} /* end wrap_get_object() */

void *
LowFive::VOLBase::
wrap_get_object(void *obj)
{
    void* res = H5VLget_object(obj, info->under_vol_id);
    return res;
}

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
    if (!wrap_ctx) return -1;
    *wrap_ctx = nullptr;
    if (!obj) return -1;
    const auto* object = static_cast<const pass_through_t*>(obj);
    auto* context = static_cast<pass_through_wrap_ctx_t*>(std::calloc(1, sizeof(pass_through_wrap_ctx_t)));
    if (!context) return -1;
    context->under_vol_id = object->under_vol_id;
    context->vol = object->vol;
    if (H5Iinc_ref(context->under_vol_id) < 0)
    {
        discard_context(context, false);
        return -1;
    }
    herr_t result;
    try
    {
        result = object->vol->get_wrap_ctx(object->under_object, &context->under_wrap_ctx);
    }
    catch (...)
    {
        discard_context(context, true);
        throw;
    }
    if (result < 0)
    {
        discard_context(context, true);
        return result;
    }
    *wrap_ctx = context;
    return result;
} /* end get_wrap_ctx() */

herr_t
LowFive::VOLBase::
get_wrap_ctx(void *obj, void **wrap_ctx)
{
    return H5VLget_wrap_ctx(obj, info->under_vol_id, wrap_ctx);
}

/*---------------------------------------------------------------------------
 *
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
    if (!obj || !_wrap_ctx) return nullptr;
    auto log = get_logger();

    pass_through_wrap_ctx_t *wrap_ctx = (pass_through_wrap_ctx_t *)_wrap_ctx;
    pass_through_t *new_obj;
    void *under;

    log->debug("------- PASS THROUGH VOL WRAP Object");

    /* Wrap the object with the underlying VOL */
    under = wrap_ctx->vol->wrap_object(obj, obj_type, wrap_ctx->under_wrap_ctx);
    if(under)
    {
        try
        {
            new_obj = new pass_through_t(under, wrap_ctx->under_vol_id, wrap_ctx->vol);
            wrap_ctx->vol->track_wrapped_object(new_obj);
        }
        catch (...)
        {
            const hid_t stack = H5Eget_current_stack();
            try { wrap_ctx->vol->unwrap_object(under); }
            catch (...) {}
            H5Eset_current_stack(stack);
            throw;
        }
    }
    else
        new_obj = NULL;

    return new_obj;
} /* end wrap_object() */

void *
LowFive::VOLBase::
wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx)
{
    void* res = H5VLwrap_object(obj, obj_type, info->under_vol_id, wrap_ctx);
    return res;
}

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
    if (!obj) return nullptr;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL UNWRAP Object");

    /* Unwrap the object with the underlying VOL */
    under = o->vol->unwrap_object(o->under_object);

    if(under)
    {
        o->vol->untrack_wrapped_object(o);
        pass_through_t::destroy_wrapper(o);
    }

    return under;
} /* end unwrap_object() */

void *
LowFive::VOLBase::
unwrap_object(void *obj)
{
    return H5VLunwrap_object(obj, info->under_vol_id);
}

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
    auto* context = static_cast<pass_through_wrap_ctx_t*>(_wrap_ctx);
    if (!context) return -1;
    const hid_t stack = H5Eget_current_stack();
    if (context->under_wrap_ctx)
    {
        herr_t result;
        try { result = context->vol->free_wrap_ctx(context->under_wrap_ctx); }
        catch (...)
        {
            H5Eclose_stack(stack);
            throw;
        }
        if (result < 0)
        {
            H5Eclose_stack(stack);
            return result;
        }
        context->under_wrap_ctx = nullptr;
    }
    if (H5Idec_ref(context->under_vol_id) < 0)
    {
        H5Eclose_stack(stack);
        return -1;
    }
    std::free(context);
    H5Eset_current_stack(stack);
    return 0;
} /* end free_wrap_ctx() */

herr_t
LowFive::VOLBase::
free_wrap_ctx(void *wrap_ctx)
{
    return H5VLfree_wrap_ctx(wrap_ctx, info->under_vol_id);
}
