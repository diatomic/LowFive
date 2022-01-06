#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    if (!unwrap(obj))
    {
        // look up in memory
        throw MetadataError("In-memory object_get() not yet implemented");
    } else
        return VOLBase::object_get(unwrap(obj), loc_params, get_type, dxpl_id, req, arguments);
}

herr_t
LowFive::MetadataVOL::
object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    return VOLBase::object_specific(unwrap(obj), loc_params, specific_type, dxpl_id, req, arguments);
}

