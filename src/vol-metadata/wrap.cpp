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
        log->trace("wrap_get_object: obj = {} (ours)", *static_cast<Object*>(obj));
    else
        log->trace("wrap_get_object: obj = {} (not ours)", fmt::ptr(obj));

    if (unwrap((Object*)obj))
        return VOLBase::wrap_get_object(unwrap((Object*)obj));
    else
        return VOLBase::wrap_get_object(obj);
}

herr_t
LowFive::MetadataVOL::
get_wrap_ctx(void *obj, void **wrap_ctx)
{
    return VOLBase::get_wrap_ctx(unwrap((Object*)obj), wrap_ctx);
}

void *
LowFive::MetadataVOL::
wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx)
{
    auto log = get_logger();
    if (ours(obj))
    {
        log->trace("wrap_object: obj = {} (ours)", *static_cast<Object*>(obj));
        return obj;
    }

    void* res = VOLBase::wrap_object(obj, obj_type, wrap_ctx);
    return res;
}

void *
LowFive::MetadataVOL::
unwrap_object(void *obj)
{
    void* res = VOLBase::unwrap_object(unwrap((Object*)obj));
    return res;
}

