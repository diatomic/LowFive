#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"

void *
LowFive::MetadataVOL::
wrap_get_object(void *obj)
{
    auto log = get_logger();
    log->trace("wrap_get_object: obj = {}", fmt::ptr(obj));
    void* our_obj = our_by_h5(obj);
    if (our_obj) {
        log->trace("wrap_get_object: obj = {} (ours)", *static_cast<Object*>(obj));
        return VOLBase::wrap_get_object(unwrap((Object*)our_obj));
    } else {
        log->trace("wrap_get_object: obj = {} (not ours)", fmt::ptr(obj));
        return VOLBase::wrap_get_object(obj);
    }
}

herr_t
LowFive::MetadataVOL::
get_wrap_ctx(void *obj, void **wrap_ctx)
{
    return VOLBase::get_wrap_ctx(unwrap((Object*)obj), wrap_ctx);
}


// always return Object*
void *
LowFive::MetadataVOL::
wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx)
{
    auto log = get_logger();
    void* our_obj = our_by_h5(obj);
    bool ours = is_ours(obj);
    if (ours)
    {
        log->trace("MetadataVOL::wrap_object, obj = {} is our Object*, return it", fmt::ptr(obj));
        return obj;
    } else if (our_obj)
    {
        log->trace("MetadataVOL::wrap_object, obj = {} is an h5_obj of our_obj = {}, return our_obj", fmt::ptr(obj), fmt::ptr(our_obj));
        return our_obj;
    } else
    {
        // obj is native HDF5 void*, need to wrap it
        // can this actually happen?
        log->trace("MetadataVOL::wrap_object, obj = {} is unknown to us native h5_obj", fmt::ptr(obj));
        void* res = VOLBase::wrap_object(obj, obj_type, wrap_ctx);
        Object* wrapper_obj = new Object(ObjectType::Wrapper, "");
        wrap(wrapper_obj, res);
        log->trace("MetadataVOL::wrap_object, returning wrapper_obj = {}", fmt::ptr(wrapper_obj));
        return res;
    }
}

void *
LowFive::MetadataVOL::
unwrap_object(void *obj)
{
    auto log = get_logger();
    bool ours = is_ours(obj);
    void* our_obj = our_by_h5(obj);
    if (ours) {
        void* wrapper_h5_obj = nullptr;
        Object* obj_ = static_cast<Object*>(obj);
        if (unwrap(obj_))
        {
            // perform unwrap on the under object
            wrapper_h5_obj = VOLBase::unwrap_object(unwrap(obj_));
        }
        // wrap the result into our wrapper
        if (wrapper_h5_obj) {
            Object* wrapper_obj = new Object(ObjectType::Wrapper, "");
            wrap(wrapper_obj, wrapper_h5_obj);
            log->trace("MetadataVOL::unwrap_object, obj = {} is ours, returning wrapper_obj = {}", *obj_, *wrapper_obj);
            return wrapper_obj;
        } else {
            log->trace("MetadataVOL::unwrap_object, obj = {} is ours, wrapper_h5_obj is NULL, return NULL", *obj_);
            return wrapper_h5_obj;
        }
    } else if (our_obj) {
        // we were given an HDF5 void*, found the corresponding Object*
        void* wrapper_h5_obj = VOLBase::unwrap_object(obj);
        if (wrapper_h5_obj) {
            Object* wrapper_obj = new Object(ObjectType::Wrapper, "");
            // wrap the result into our wrapper
            wrap(wrapper_obj, wrapper_h5_obj);
            log->trace("MetadataVOL::unwrap_object, obj = {} is h5_obj of our_obj = {}, returning wrapper_obj = {}", fmt::ptr(obj), *static_cast<Object*>(our_obj), *wrapper_obj);
            return wrapper_obj;
        } else {
            log->trace("MetadataVOL::unwrap_object, obj = {} is h5_obj of our_obj = {}, VOLBase returned NULL on obj, returning NULL", fmt::ptr(obj), *static_cast<Object*>(our_obj));
            return wrapper_h5_obj;
        }
    } else
    {
        throw MetadataError("unexpected HDF5 object in MetadataVOL::unwrap_object");
        return VOLBase::unwrap_object(obj);
    }
}

