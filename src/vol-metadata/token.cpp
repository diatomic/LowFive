#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"

herr_t
LowFive::MetadataVOL::
token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    auto log = get_logger();
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    log->trace("token_compare: {} ({} {}) {} {}", fmt::ptr(obj), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), fmt::ptr(token1), fmt::ptr(token2));
    if (!unwrap(obj))           // look up in memory
    {
        void* p1;
        void* p2;
        memcpy(&p1, token1->__data, sizeof(void*));
        memcpy(&p2, token2->__data, sizeof(void*));
        log->trace("token_compare: comparing {} {}", fmt::ptr(p1), fmt::ptr(p2));

        *cmp_value = memcmp(token1->__data, token2->__data, sizeof(void*));
        log->trace("token_compare: result = {}", *cmp_value);
        return 0;
    } else
    {
        auto res = VOLBase::token_cmp(unwrap(obj), token1, token2, cmp_value);
        log->trace("token_compare: result = {}", *cmp_value);
        return res;
    }
}
