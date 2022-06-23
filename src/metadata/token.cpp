#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"

herr_t
LowFive::MetadataVOL::
token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    auto log = get_logger();
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    log->trace("{} ({} {}) {} {}", fmt::ptr(obj), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), fmt::ptr(token1), fmt::ptr(token2));
    if (!unwrap(obj))           // look up in memory
    {
        *cmp_value = memcmp(token1, token2, sizeof(H5O_token_t));
        log->trace("result = {}", *cmp_value);
        return 0;
    } else
        return VOLBase::token_cmp(unwrap(obj), token1, token2, cmp_value);
}
