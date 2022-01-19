#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    fmt::print(stderr, "{} ({} {}) {} {}\n", fmt::ptr(obj), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), fmt::ptr(token1), fmt::ptr(token2));
    if (!unwrap(obj))           // look up in memory
    {
        *cmp_value = memcmp(token1, token2, sizeof(H5O_token_t));
        fmt::print(stderr, "result = {}\n", *cmp_value);
        return 0;
    } else
        return VOLBase::token_cmp(unwrap(obj), token1, token2, cmp_value);
}
