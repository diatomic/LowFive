#include <lowfive/vol-metadata.hpp>


void *
LowFive::MetadataVOL::
wrap_get_object(void *obj)
{
//     fprintf(stderr, "wrap_get_object obj = %p unwrap(obj) = %p\n", obj, unwrap(obj));

    return VOLBase::wrap_get_object(unwrap(obj));
}

herr_t
LowFive::MetadataVOL::
get_wrap_ctx(void *obj, void **wrap_ctx)
{
//     fprintf(stderr, "get_wrap_ctx obj = %p unwrap(obj) = %p\n", obj, unwrap(obj));

    return VOLBase::get_wrap_ctx(unwrap(obj), wrap_ctx);
}

void *
LowFive::MetadataVOL::
wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx)
{
    return wrap(VOLBase::wrap_object(obj, obj_type, wrap_ctx));
}

void *
LowFive::MetadataVOL::
unwrap_object(void *obj)
{
    void* res = VOLBase::unwrap_object(unwrap(obj));
    if (res)
        return wrap(res);
    else
        return res;
}

