#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <assert.h>

void *
LowFive::MetadataVOL::
object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    auto* parent = static_cast<Object*>(pointers->mdata_obj);
    *opened_type = H5I_BADID;
    Object* metadata = nullptr;
    if (parent)
    {
        auto path = parent->locate(*loc_params);
        metadata = path.path.empty() ? path.obj : nullptr;
    }
    void* underlying = nullptr;
    if (unwrap(obj))
    {
        underlying = VOLBase::object_open(unwrap(obj), loc_params, opened_type, dxpl_id, req);
        if (!underlying) return nullptr;
        if (parent && !metadata && loc_params->type == H5VL_OBJECT_BY_NAME)
        {
            auto close_underlying = [&]()
            {
                if (*opened_type == H5I_DATASET)
                    return VOLBase::dataset_close(underlying, dxpl_id, nullptr);
                if (*opened_type == H5I_GROUP)
                    return VOLBase::group_close(underlying, dxpl_id, nullptr);
                if (*opened_type == H5I_DATATYPE)
                    return VOLBase::datatype_close(underlying, dxpl_id, nullptr);
                return herr_t {-1};
            };

            Object* inserted = nullptr;
            if (*opened_type == H5I_DATASET)
            {
                auto properties = query_dataset_metadata(
                    [&](H5VL_dataset_get_args_t& args)
                    {
                        return VOLBase::dataset_get(
                            underlying, &args, dxpl_id, nullptr);
                    });
                if (!properties)
                {
                    close_underlying();
                    return nullptr;
                }
                inserted = new Dataset(
                    "", properties->type.id, properties->space.id,
                    Dataset::Ownership::lowfive, properties->dcpl,
                    properties->dapl, true, false);
            }
            else if (*opened_type == H5I_GROUP)
                inserted = new DummyObject(ObjectType::Group, "");
            else if (*opened_type == H5I_DATATYPE)
                inserted = new DummyObject(ObjectType::NamedDtype, "");
            else
            {
                close_underlying();
                return nullptr;
            }

            const char* name = loc_params->loc_data.loc_by_name.name;
            auto target = metadata_target_path(parent, name);
            if (target.obj && !target.path.empty())
                metadata = target.obj->add_path(inserted, target.path);
            else
                delete inserted;
            if (!metadata)
            {
                close_underlying();
                return nullptr;
            }
        }
    }
    else
    {
        if (!metadata) return nullptr;
        *opened_type = get_identifier_type(metadata);
        if (*opened_type == H5I_FILE) *opened_type = H5I_GROUP;
        if (*opened_type != H5I_GROUP && *opened_type != H5I_DATASET && *opened_type != H5I_DATATYPE)
        {
            *opened_type = H5I_BADID;
            return nullptr;
        }
    }
    auto* result = wrap(underlying);
    result->mdata_obj = metadata ? metadata : parent;
    result->access_flags = pointers->access_flags;
    return result;
}

herr_t
LowFive::MetadataVOL::
object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    if (!static_cast<ObjectPointers*>(dst_obj)->writable()) return -1;
    auto log = get_logger();

    if (!unwrap(src_obj) && !unwrap(dst_obj))   // memory
    {
        ObjectPointers* src_obj_        = (ObjectPointers*) src_obj; (void) src_obj_;
        ObjectPointers* dst_obj_        = (ObjectPointers*) dst_obj;
        Object*         dst_mdata_obj   = static_cast<Object*>(dst_obj_->mdata_obj); (void) dst_mdata_obj;

        // TODO
        throw MetadataError(fmt::format("object_copy(): not implemented in metadata yet"));
    }
    else                                        // passthru
    {
        ObjectPointers* src_obj_        = (ObjectPointers*) src_obj;
        ObjectPointers* dst_obj_        = (ObjectPointers*) dst_obj;

        // warn if metadata are available but won't be used
        if (src_obj_->mdata_obj && dst_obj_->mdata_obj)
            log->warn("Warning: object_copy: src_obj = {} dst_obj = {} metadata are available but won't be used", *src_obj_, *dst_obj_);

        return VOLBase::object_copy(unwrap(src_obj), src_loc_params, src_name, unwrap(dst_obj), dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req);
    }
}

herr_t
LowFive::MetadataVOL::
object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t* args, hid_t dxpl_id, void **req)
{
    auto get_type = args->op_type;
    auto log = get_logger();
    log->trace("object_get: get_type {}", get_type);
    if (!unwrap(obj))           // look up in memory
    {
        // The following is adapted from HDF5's H5VL__native_object_get() in H5VLnative_object.c

        ObjectPointers* obj_        = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

        // map of our object types to hdf5 object types
        //std::vector<int> h5_types = {H5O_TYPE_UNKNOWN, H5O_TYPE_GROUP, H5O_TYPE_DATASET, H5O_TYPE_UNKNOWN, H5O_TYPE_NAMED_DATATYPE};
        std::vector<int> h5_types = {H5O_TYPE_GROUP, H5O_TYPE_GROUP, H5O_TYPE_DATASET, H5O_TYPE_UNKNOWN, H5O_TYPE_NAMED_DATATYPE};

        switch (get_type)
        {
        case H5VL_OBJECT_GET_FILE:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_FILE");

            if (loc_params->type == H5VL_OBJECT_BY_SELF)
            {
                log->trace("object_get: loc_params->type = H5VL_OBJECT_BY_SELF");

                Object* res = mdata_obj->find_root();
                assert(res->type == ObjectType::File);
                auto* res_pair = wrap(nullptr);
                res_pair->mdata_obj = res;
                res_pair->tmp = true;
                res_pair->file_wrapper = true;
                res_pair->reference_file_wrapper =
                    retaining_reference_file;
                res_pair->access_flags = obj_->access_flags;
                log->trace("MetadataVOL: temporary file = {}, mdata = {}",
                           fmt::ptr(res_pair), fmt::ptr(res));
                *args->args.get_file.file = res_pair;
            }
            else
                throw MetadataError("object_get() unrecognized loc_params->type");

            return 0;
        }

        case H5VL_OBJECT_GET_NAME:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_NAME");

            // TODO: should we set actual size in ret and use min(ret, buf_size) in strncpy ?
            size_t *ret    = args->args.get_name.name_len;
            char *name      = args->args.get_name.buf;
            size_t size     = args->args.get_name.buf_size;

            Object* obj = mdata_obj->locate(*loc_params).exact();
            if (!obj) return -1;
            auto path = obj->fullname().second;
            *ret = path.size();
            if (name && size)
            {
                auto copied = std::min(size - 1, path.size());
                memcpy(name, path.data(), copied);
                name[copied] = '\0';
            }

            return 0;
        }

        case H5VL_OBJECT_GET_TYPE:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_TYPE");

            H5O_type_t *obj_type = args->args.get_type.obj_type;

            Object* obj = mdata_obj->locate(*loc_params).exact();
            if (static_cast<int>(obj->type) >= h5_types.size())     // sanity check
                throw MetadataError(fmt::format("object_get(): obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", obj->type));

            int otype   = h5_types[static_cast<int>(obj->type)];
            *obj_type   = static_cast<H5O_type_t>(otype);
            log->trace("object_get: obj->type {} hdf5 otype {}", obj->type, otype);

            if (otype == H5O_TYPE_UNKNOWN)
                throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));

            return 0;
        }

        case H5VL_OBJECT_GET_INFO:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_INFO");

            H5O_info2_t  *oinfo = args->args.get_info.oinfo; // H5O_info2_t defined in H5Opublic.h
            // we don't fill args->args.get_info.fields

            Object* obj = mdata_obj->locate(*loc_params).exact();

            if (static_cast<int>(obj->type) >= h5_types.size())     // sanity check
                throw MetadataError(fmt::format("object_get(): mdata_obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", mdata_obj->type));

            // get object type in HDF format
            int otype   = h5_types[static_cast<int>(obj->type)];
            oinfo->type = static_cast<H5O_type_t>(otype);
            log->trace("object_get: name={} obj->type {} hdf5 otype {}", obj->name, obj->type, otype);

            if (otype == H5O_TYPE_UNKNOWN)
                throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));

            obj->fill_token(oinfo->token);

            // count number of attributes
            oinfo->num_attrs = 0;
            for (auto& c : obj->children)
            {
                if (c->type == LowFive::ObjectType::Attribute)
                    oinfo->num_attrs++;
            }

            // TODO: following are uninitialized, unknown, arbitrary
            oinfo->fileno   = 0;                                // TODO
            oinfo->rc       = 0;                                // TODO
            oinfo->atime    = 0;                                // TODO
            oinfo->mtime    = 0;                                // TODO
            oinfo->ctime    = 0;                                // TODO
            oinfo->btime    = 0;                                // TODO

            log->trace("*** ------------------- ***");
            log->trace("Warning: getting object info not fully implemented yet.");
            log->trace("Ignoring file number, reference counts, times.");
            log->trace("*** ------------------- ***");

            return 0;
        }

        default:
            throw MetadataError("object_get(): unrecognized get_type");
        }
    } else
    {
        auto* wrapped = static_cast<ObjectPointers*>(obj);
        auto result =
            VOLBase::object_get(unwrap(obj), loc_params, args, dxpl_id, req);
        if (result >= 0 && get_type == H5VL_OBJECT_GET_FILE &&
            loc_params->type == H5VL_OBJECT_BY_SELF && wrapped->mdata_obj &&
            *args->args.get_file.file)
            pending_wrap_metadata[*args->args.get_file.file] = {
                static_cast<Object*>(wrapped->mdata_obj)->find_root(),
                wrapped->access_flags
            };
        else if (wrapped->mdata_obj)
            log->warn("Warning: object_get: obj = {} metadata are available "
                      "but won't be used", *wrapped);
        return result;
    }
}

herr_t
LowFive::MetadataVOL::
object_specific(void *obj, const H5VL_loc_params_t *loc_params,
        H5VL_object_specific_args_t* args, hid_t dxpl_id, void **req)
{
    auto specific_type = args->op_type;

    auto log = get_logger();
    log->trace("object_specific: specific_type = {}", specific_type);
    if (!unwrap(obj))
    {
        ObjectPointers* obj_ = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);
        log->trace("filling token, obj = {}", *obj_);

        switch(specific_type)
        {
            /* H5Oexists_by_name */
        case H5VL_OBJECT_EXISTS: {

            auto location = mdata_obj->locate(*loc_params);
            if (location.obj && location.path.empty())
                *(args->args.exists.exists) = 1;
            else
                *(args->args.exists.exists) = 0;

            return 0;
            break;
        }
            /* Lookup object */
        case H5VL_OBJECT_LOOKUP: {
            log->trace("loc_params->type = {}", loc_params->type);
            log->trace("name = {}", loc_params->loc_data.loc_by_name.name);

            Object* o = mdata_obj->locate(*loc_params).exact();
            if (!o) return -1;
            o->fill_token(*args->args.lookup.token_ptr);
            return 0;

            break;
        }
        default:
            throw MetadataError("object_specific(): unrecognized specific_type");
        }
    } else
    {
        ObjectPointers* obj_        = (ObjectPointers*) obj;

        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: object_specific: obj = {} metadata are available but won't be used", *obj_);

        return VOLBase::object_specific(unwrap(obj), loc_params, args, dxpl_id, req);
    }
}

herr_t
LowFive::MetadataVOL::
object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    auto op_type = args->op_type;

    if (!unwrap(obj))               // memory
    {
        ObjectPointers* obj_        = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj); (void)mdata_obj;

        // see HDF5's H5VL__native_object_optional() in H5VLnative_object.c
        // TODO: not implemented yet
        switch(op_type)
        {
            // H5Oget_comment / H5Oget_comment_by_name
        case H5VL_NATIVE_OBJECT_GET_COMMENT:
        {
            // TODO
            throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_GET_COMMENT not implemented in metadata yet"));
        }

            // H5Oset_comment
        case H5VL_NATIVE_OBJECT_SET_COMMENT:
        {
            // TODO
            //throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_SET_COMMENT not implemented in metadata yet"));
            log->warn("OBJECT_SET_COMMENT not implemented in object_optional; skipping silently");
            return 0;
        }

            // H5Odisable_mdc_flushes
        case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
        {
            // TODO
            throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES not implemented in metadata yet"));
        }

            // H5Oenable_mdc_flushes
        case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
        {
            // TODO
            throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES not implemented in metadata yet"));
        }

            // H5Oare_mdc_flushes_disabled
        case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
        {
            // TODO
            throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED not implemented in metadata yet"));
        }

            // H5Oget_native_info(_name|_by_idx)
        case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
        {
//            H5O_native_info_t *native_info = args->args va_arg(arguments, H5O_native_info_t *);
//            unsigned           fields      = va_arg(arguments, unsigned);

//            log->trace("get_native_info: fields = {}", fields);
//            log->warn("get_native_info not implemented, nothing is set");

            // TODO
            //throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_GET_NATIVE_INFO not implemented in metadata yet"));
            return 0;
        }

        default:
            throw MetadataError(fmt::format("object_optional: invalid optional type"));
        }

    }
    else                            // passthru
    {
        ObjectPointers* obj_        = (ObjectPointers*) obj;

        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: object_optional: obj = {} metadata are available but won't be used", *obj_);

        return VOLBase::object_optional(unwrap(obj), loc_params, args, dxpl_id, req);
    }
}
