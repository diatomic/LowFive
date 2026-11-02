#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"

void *
LowFive::MetadataVOL::
wrap_get_object(void *obj)
{
    auto log = get_logger();
    bool our = ours(obj);
    if (our)
        log->trace("wrap_get_object: obj = {}", *static_cast<ObjectPointers*>(obj));
    else
        log->trace("wrap_get_object: obj = {}", fmt::ptr(obj));

    if (unwrap(obj))
        return VOLBase::wrap_get_object(unwrap(obj));
    else
        return VOLBase::wrap_get_object(obj);
}

herr_t
LowFive::MetadataVOL::
get_wrap_ctx(void *obj, void **wrap_ctx)
{
    return VOLBase::get_wrap_ctx(unwrap(obj), wrap_ctx);
}

void *
LowFive::MetadataVOL::
wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx)
{
    auto log = get_logger();
    if (ours(obj))
    {
        log->trace("wrap_object: obj = {}", *static_cast<ObjectPointers*>(obj));
        return obj;
    }

    void* res = VOLBase::wrap_object(obj, obj_type, wrap_ctx);
    auto* wrapped = wrap(res);
    wrapped->file_wrapper = obj_type == H5I_FILE;
    wrapped->reference_file_wrapper =
        wrapped->file_wrapper && retaining_reference_file;
    auto pending = pending_wrap_metadata.find(obj);
    if (pending != pending_wrap_metadata.end())
    {
        wrapped->mdata_obj = pending->second.first;
        wrapped->access_flags = pending->second.second;
        pending_wrap_metadata.erase(pending);
    }
    return wrapped;
}

void *
LowFive::MetadataVOL::
unwrap_object(void *obj)
{
    void* res = VOLBase::unwrap_object(unwrap(obj));
    if (res)
        drop(obj);
    return res;
}

