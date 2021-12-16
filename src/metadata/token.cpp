#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    return VOLBase::token_cmp(unwrap(obj), token1, token2, cmp_value);
}
