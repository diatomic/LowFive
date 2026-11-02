#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"


void*
LowFive::MetadataVOL::
datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                hid_t tapl_id, hid_t dxpl_id, void **req)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    if (!pointers->writable()) return nullptr;

    auto* current = static_cast<Object*>(pointers->mdata_obj);
    Object* parent = nullptr;
    Object::ObjectPath destination {nullptr, ""};
    try
    {
        if (current)
        {
            parent = current->locate(*loc_params).exact();
            if (!parent) return nullptr;
            destination = metadata_target_path(parent, name);
            if (!destination.obj || destination.path.empty())
                return nullptr;
            if (!unwrap(pointers) && !destination.is_name())
                return nullptr;
        }
    }
    catch (...) { return nullptr; }

    void* underlying = nullptr;
    if (unwrap(pointers))
    {
        underlying = VOLBase::datatype_commit(
            unwrap(pointers), loc_params, name, type_id, lcpl_id, tcpl_id,
            tapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    if (!underlying && !destination.obj) return nullptr;

    auto* result = wrap(underlying);
    result->access_flags = pointers->access_flags;
    if (destination.obj)
        result->mdata_obj = destination.obj->add_path(
            new CommittedDatatype("", type_id), destination.path);
    if (destination.obj && !result->mdata_obj)
    {
        if (underlying) VOLBase::datatype_close(underlying, dxpl_id, nullptr);
        drop(result);
        return nullptr;
    }
    return result;
}

herr_t
LowFive::MetadataVOL::
datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    ObjectPointers* dt_ = (ObjectPointers*) dt;
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
    auto* pointers = static_cast<ObjectPointers*>(obj);
    void* underlying = unwrap(pointers) ?
        VOLBase::datatype_open(
            unwrap(pointers), loc_params, name, tapl_id, dxpl_id, req) :
        nullptr;

    CommittedDatatype* metadata = nullptr;
    Object::ObjectPath target {nullptr, ""};
    if (auto* current = static_cast<Object*>(pointers->mdata_obj))
    {
        try
        {
            auto* parent = current->locate(*loc_params).exact();
            if (parent)
            {
                target = metadata_target_path(parent, name);
                if (target.path.empty())
                    metadata = dynamic_cast<CommittedDatatype*>(target.obj);
            }
        }
        catch (...) {}
    }
    const bool can_cache =
        target.obj && (!target.path.empty() ||
                       (target.obj->type == ObjectType::NamedDtype &&
                        dynamic_cast<DummyObject*>(target.obj)));
    if (underlying && !metadata && can_cache)
    {
        H5VL_datatype_get_args_t get {};
        size_t binary_size = 0;
        get.op_type = H5VL_DATATYPE_GET_BINARY_SIZE;
        get.args.get_binary_size.size = &binary_size;
        if (VOLBase::datatype_get(underlying, &get, dxpl_id, nullptr) >= 0)
        {
            auto cached = std::make_unique<CommittedDatatype>("", 0);
            cached->data.resize(binary_size);
            get.op_type = H5VL_DATATYPE_GET_BINARY;
            get.args.get_binary.buf = cached->data.data();
            get.args.get_binary.buf_size = cached->data.size();
            if (VOLBase::datatype_get(underlying, &get, dxpl_id, nullptr) >= 0)
            {
                Object* inserted = nullptr;
                if (target.path.empty())
                {
                    auto* file = dynamic_cast<File*>(target.obj->find_root());
                    if (file)
                        inserted = replace_metadata(
                            *this, *file, target.obj, cached.release());
                }
                else
                    inserted = target.obj->add_path(
                        cached.release(), target.path);
                metadata = dynamic_cast<CommittedDatatype*>(inserted);
            }
        }
    }
    if (!underlying && !metadata) return nullptr;
    if (underlying && target.obj && !metadata)
    {
        VOLBase::datatype_close(underlying, dxpl_id, nullptr);
        return nullptr;
    }

    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->access_flags = pointers->access_flags;
    return result;
}

herr_t
LowFive::MetadataVOL::datatype_get(void* dt, H5VL_datatype_get_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*) dt;
    if (unwrap(obj_))
        return VOLBase::datatype_get(unwrap(obj_), args, dxpl_id, req);

    auto* o = static_cast<CommittedDatatype*>(obj_->mdata_obj);
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
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_specific(unwrap(obj_), args, dxpl_id, req);

    // nothing to do in-memory
    return 0;
}

herr_t
LowFive::MetadataVOL::datatype_optional(void* obj, H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (unwrap(obj_))
        return VOLBase::datatype_optional(unwrap(obj_), args, dxpl_id, req);

    // nothing to do in-memory
    return 0;
}
