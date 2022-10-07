#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"

herr_t
LowFive::MetadataVOL::
blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    if (!unwrap(obj))
        throw MetadataError(fmt::format("blob_put() not implemented in metadata-only mode"));
    return VOLBase::blob_put(unwrap(obj), buf, size, blob_id, ctx);
}

herr_t
LowFive::MetadataVOL::
blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments)
{
    if (!unwrap(obj))
        throw MetadataError(fmt::format("blob_specific() not implemented in metadata-only mode"));
    return VOLBase::blob_specific(unwrap(obj), blob_id, specific_type, arguments);
}

herr_t
LowFive::MetadataVOL::
blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    if (!unwrap(obj))
        throw MetadataError(fmt::format("blob_get() not implemented in metadata-only mode"));
    return VOLBase::blob_get(unwrap(obj), blob_id, buf, size, ctx);
}
