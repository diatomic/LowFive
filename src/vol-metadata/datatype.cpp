#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"

void*
LowFive::MetadataVOL::
datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                hid_t tapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return wrap(VOLBase::datatype_commit(unwrap(obj_), loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req));

    throw MetadataError("datatype_commit not implemented in-memory");
}

herr_t
LowFive::MetadataVOL::
datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    ObjectPointers* dt_ = (ObjectPointers*) dt;
    if (unwrap(dt_))
        return VOLBase::datatype_close(unwrap(dt_), dxpl_id, req);

    throw MetadataError("datatype_close not implemented in-memory");
}

void *
LowFive::MetadataVOL::
datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return wrap(VOLBase::datatype_open(unwrap(obj_), loc_params, name, tapl_id, dxpl_id, req));

    throw MetadataError("datatype_open not implemented in-memory");
}

herr_t
LowFive::MetadataVOL::datatype_get(void* dt, H5VL_datatype_get_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*) dt;
    if (unwrap(obj_))
        return VOLBase::datatype_get(unwrap(obj_), args, dxpl_id, req);

    throw MetadataError("datatype_get not implemented in-memory");
}

herr_t
LowFive::MetadataVOL::datatype_specific(void* obj, H5VL_datatype_specific_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_specific(unwrap(obj_), args, dxpl_id, req);

    throw MetadataError("datatype_specific not implemented in-memory");
}

herr_t
LowFive::MetadataVOL::datatype_optional(void* obj, H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_optional(unwrap(obj_), args, dxpl_id, req);

    throw MetadataError("datatype_optional not implemented in-memory");
}