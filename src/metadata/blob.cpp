#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    return VOLBase::blob_put(unwrap(obj), buf, size, blob_id, ctx);
}

