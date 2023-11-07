#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"


void*
LowFive::MetadataVOL::
datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                hid_t tapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    Object* obj_ = (Object*) obj;

    if (!obj_)
        throw std::runtime_error("MetdataVOL::datatype_commit, unexpected");

    auto* parent = obj_->locate(*loc_params).exact();
    auto obj_path = parent->search(name);
    Object* result = obj_path.obj->add_child(new CommittedDatatype(obj_path.path, type_id));
    log->trace("Committing datatype under {}, name = {}", parent->name, name);

    if (unwrap(obj_))
        wrap(result, VOLBase::datatype_commit(unwrap(obj_), loc_params, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req));

   return result;
}

herr_t
LowFive::MetadataVOL::
datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    Object* dt_ = (Object*) dt;
    if (unwrap(dt_))
        return VOLBase::datatype_close(unwrap(dt_), dxpl_id, req);

    // do nothing; keep the type in memory
    return 0;
}

void *
LowFive::MetadataVOL::
datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    Object* obj_ = (Object*) obj;
    Object* result = nullptr;

    auto log = get_logger();

    auto* o = static_cast<Object*>(obj_);
    if (o)
    {
        try
        {
            auto* parent = o->locate(*loc_params).exact();
            result = parent->search(name).exact();
        } catch(...)
        {
            log->critical("datatype_open, exception suppressed");
        }
    }

    if (unwrap(obj_))
        wrap(result, VOLBase::datatype_open(unwrap(obj_), loc_params, name, tapl_id, dxpl_id, req));

    return result;
}

herr_t
LowFive::MetadataVOL::datatype_get(void* dt, H5VL_datatype_get_args_t* args, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*) dt;
    if (unwrap(obj_))
        return VOLBase::datatype_get(unwrap(obj_), args, dxpl_id, req);

    auto* o = dynamic_cast<CommittedDatatype*>(obj_);
    assert(o);

    switch (args->op_type) {
        /* H5T_construct_datatype (library private routine) */
        case H5VL_DATATYPE_GET_BINARY_SIZE: {
            *(args->args.get_binary_size.size) = o->data.size();
            break;
        }

        /* H5T_construct_datatype (library private routine) */
        case H5VL_DATATYPE_GET_BINARY: {
            memcpy(args->args.get_binary.buf, o->data.data(), args->args.get_binary.buf_size);
            break;
        }

        /* H5Tget_create_plist */
        case H5VL_DATATYPE_GET_TCPL: {
            throw MetadataError("datatype_get: H5VL_DATATYPE_GET_TCPL not implemented in-memory");
        }

        default:
            throw MetadataError("datatype_get: unrecognized op_type");
    } /* end switch */

    return 0;
}

herr_t
LowFive::MetadataVOL::datatype_specific(void* obj, H5VL_datatype_specific_args_t* args, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_specific(unwrap(obj_), args, dxpl_id, req);

    // nothing to do in-memory
    return 0;
}

herr_t
LowFive::MetadataVOL::datatype_optional(void* obj, H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_optional(unwrap(obj_), args, dxpl_id, req);

    // nothing to do in-memory
    return 0;
}
