#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <assert.h>

void *
LowFive::MetadataVOL::
object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    Object* obj_        = (Object*) obj;

    auto log = get_logger();
    log->trace("MetadataVOL::object_open: parent obj = {}, loca_params.type = {}", *obj_, loc_params->type);

    *opened_type = H5I_BADID;

    Object* result = nullptr;
    void* result_h5_obj = nullptr;
    if (unwrap(obj_))
    {
        // passthru
        result_h5_obj = VOLBase::object_open(unwrap(obj_), loc_params, opened_type, dxpl_id, req);
    }

    log->trace("MetadataVOL::object_open, result = {}", fmt::ptr(result));

    Object*         mdata_obj   = obj_;

    // TODO: technically it's even more complicated; it's possible that obj has mdata, but the object we are opening doesn't;
    //       I think locate will return the last parent that has mdata, which is not what we want
    if (mdata_obj)
    {
        auto op = mdata_obj->locate(*loc_params);
        auto fullname = op.obj->fullname(op.path);
        log->trace("MetadataVOL::object_open: fullname = ({},{}), locate = ({}, {})", fullname.first, fullname.second, fmt::ptr(op.obj), op.path);
        if (op.path.empty())
        {
            Object* o = op.obj;
            log->trace("MetadataVOL::object_open, result = {}, mdata_obj = {}", fmt::ptr(result), fmt::ptr(o));
            result = o;

            if (*opened_type == H5I_BADID)      // not set by native
            {
                *opened_type = get_identifier_type(o);

                log->trace("MetadataVOL::object_open(): opened_type was H5I_BAD_ID, reset to type {}", *opened_type);

                // XXX: this is hack; we should not be able to open a file, but
                //      rather the "/" group. Ideally, we'd have such a group for every
                //      file, but I'm not sure how to implement that. This is a
                //      workaround.
                if (*opened_type == H5I_FILE)
                {
                    log->warn("In MetadataVOL::object_open(): forcing file to be a group");
                    *opened_type = H5I_GROUP;
                }
            }

            // HDF5 allows only a group, dataset, or datatype to be opened as an abject
            if (*opened_type != H5I_GROUP && *opened_type != H5I_DATATYPE && *opened_type != H5I_DATASET)
            {
                // TODO: consider throwing an error if this should never happen?
                log->trace("MetadataVOL: object_open(): attempting to open object mdata type {}, which is not a group, dataset, or datatype; setting opened_type to H5I_BADID and returning null");
                *opened_type = H5I_BADID;
                result = nullptr;
            }
        } else
        {
            // we didn't find the object; let's make sure that's Ok
            log_assert(!match_any(fullname, memory), "didn't find an mdata_obj");
        }
    }

    // wrap under h5_obj into ours
    if (result)
        wrap(result, result_h5_obj);

    log->trace("MetadataVOL::object_open: opened_type = {}", *opened_type);

    return result;
}

herr_t
LowFive::MetadataVOL::
object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    Object* src_obj_        = (Object*) src_obj;
    Object* dst_obj_        = (Object*) dst_obj;

    if (!unwrap(src_obj_) && !unwrap(dst_obj_))   // memory
    {
        Object*         dst_mdata_obj   = static_cast<Object*>(dst_obj_); (void) dst_mdata_obj;

        // TODO
        throw MetadataError(fmt::format("object_copy(): not implemented in metadata yet"));
    }
    else                                        // passthru
        return VOLBase::object_copy(unwrap(src_obj_), src_loc_params, src_name, unwrap(dst_obj_), dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t* args, hid_t dxpl_id, void **req)
{
    auto get_type = args->op_type;
    auto log = get_logger();
    log->trace("object_get: get_type {}", get_type);
    Object*  mdata_obj   = (Object*)obj;
    if (!unwrap(mdata_obj))           // look up in memory
    {
        // The following is adapted from HDF5's H5VL__native_object_get() in H5VLnative_object.c


        // map of our object types to hdf5 object types
        std::vector<int> h5_types = {H5O_TYPE_UNKNOWN, H5O_TYPE_GROUP, H5O_TYPE_DATASET, H5O_TYPE_UNKNOWN, H5O_TYPE_NAMED_DATATYPE};

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
                *args->args.get_file.file = res;
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

            if (loc_params->type == H5VL_OBJECT_BY_SELF)
                strncpy(name, mdata_obj->name.c_str(), size);
            else if (loc_params->type == H5VL_OBJECT_BY_TOKEN)
            {
                log->trace("object_get: loc_params->type = H5VL_OBJECT_BY_TOKEN");

                // TODO: ignoring the token and just getting the name of the current object
                strncpy(name, mdata_obj->name.c_str(), size);
            }
            else
                throw MetadataError("object_get() unrecognized loc_params->type");

            return 0;
        }

        case H5VL_OBJECT_GET_TYPE:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_TYPE");

            H5O_type_t *obj_type = args->args.get_type.obj_type;

            if (loc_params->type == H5VL_OBJECT_BY_TOKEN)
            {
                log->trace("object_get: loc_params->type = H5VL_OBJECT_BY_TOKEN");

                if (static_cast<int>(mdata_obj->type) >= h5_types.size())     // sanity check
                    throw MetadataError(fmt::format("object_get(): mdata_obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", mdata_obj->type));

                // TODO: ignoring the token and just getting the type of the current object
                int otype   = h5_types[static_cast<int>(mdata_obj->type)];
                *obj_type   = static_cast<H5O_type_t>(otype);
                log->trace("object_get: mdata_obj->type {} hdf5 otype {}", mdata_obj->type, otype);

                if (otype == H5O_TYPE_UNKNOWN)
                    throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));
            }
            else
                throw MetadataError("object_get() unrecognized loc_params->type");

            return 0;
        }

        case H5VL_OBJECT_GET_INFO:
        {
            log->trace("object_get: get_type = H5VL_OBJECT_GET_INFO");

            H5O_info2_t  *oinfo = args->args.get_info.oinfo; // H5O_info2_t defined in H5Opublic.h
            // we don't fill args->args.get_info.fields

            if (loc_params->type == H5VL_OBJECT_BY_SELF)            // H5Oget_info
            {
                log->trace("object_get: loc_params->type = H5VL_OBJECT_BY_SELF");

                if (static_cast<int>(mdata_obj->type) >= h5_types.size())     // sanity check
                    throw MetadataError(fmt::format("object_get(): mdata_obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", mdata_obj->type));

                // get object type in HDF format
                int otype   = h5_types[static_cast<int>(mdata_obj->type)];
                oinfo->type = static_cast<H5O_type_t>(otype);
                log->trace("object_get: mdata_obj->type {} hdf5 otype {}", mdata_obj->type, otype);

                if (otype == H5O_TYPE_UNKNOWN)
                {
//                         throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));
                    fmt::print(stderr, "object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; returning -1\n");
                    return -1;
                }

                mdata_obj->fill_token(oinfo->token);

                // count number of attributes
                oinfo->num_attrs = 0;
                for (auto& c : mdata_obj->children)
                {
                    if (c->type == LowFive::ObjectType::Attribute)
                        oinfo->num_attrs++;
                }

            }
            else if (loc_params->type == H5VL_OBJECT_BY_NAME)       // H5Oget_info_by_name
            {
                log->trace("loc_params->type = H5VL_OBJECT_BY_NAME");

                // TP: I think this means obj points to the group and we search for the name under direct(?) children of obj?
                // the name is taken from loc_data.loc_by_name.name
                bool found = false;
                for (auto& o : mdata_obj->children)
                {
                    if (o->name.c_str() == loc_params->loc_data.loc_by_name.name)
                    {
                        found = true;

                        if (static_cast<int>(mdata_obj->type) >= h5_types.size())     // sanity check
                            throw MetadataError(fmt::format("object_get(): mdata_obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", mdata_obj->type));

                        // get object type in HDF format
                        int otype   = h5_types[static_cast<int>(o->type)];
                        oinfo->type = static_cast<H5O_type_t>(otype);
                        log->trace("object_get: child mdata o->type {} hdf5 otype {}", o->type, otype);

                        if (otype == H5O_TYPE_UNKNOWN)
                            throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));

                        o->fill_token(oinfo->token);

                        // count number of attributes
                        oinfo->num_attrs = 0;
                        for (auto& c : o->children)
                        {
                            if (c->type == LowFive::ObjectType::Attribute)
                                oinfo->num_attrs++;
                        }

                        break;
                    }
                }

                if (!found)
                    throw MetadataError("object_get() info by name did not find matching name");
            }
            else if (loc_params->type == H5VL_OBJECT_BY_IDX)        // H5Oget_info_by_idx
            {
                log->trace("loc_params->type = H5VL_OBJECT_BY_IDX");

                // TP: I think this means obj points to the group and we search for the name under direct(?) children of obj?
                // the name is taken from loc_data.loc_by_idx.name
                bool found = false;
                for (auto& o : mdata_obj->children)
                {
                    if (o->name.c_str() == loc_params->loc_data.loc_by_idx.name)
                    {
                        found = true;

                        if (static_cast<int>(mdata_obj->type) >= h5_types.size())     // sanity check
                            throw MetadataError(fmt::format("object_get(): mdata_obj->type {} > H5O_TYPE_NAMED_DATATYPE, the last element of h5_types", mdata_obj->type));

                        // get object type in HDF format
                        int otype   = h5_types[static_cast<int>(o->type)];
                        oinfo->type = static_cast<H5O_type_t>(otype);
                        log->trace("object_get: child mdata o->type {} hdf5 otype {}", o->type, otype);

                        if (otype == H5O_TYPE_UNKNOWN)
                            throw MetadataError(fmt::format("object_get(): hdf5 otype = H5O_TYPE_UNKNOWN; this should not happen"));

                        o->fill_token(oinfo->token);

                        // count number of attributes
                        oinfo->num_attrs = 0;
                        for (auto& c : o->children)
                        {
                            if (c->type == LowFive::ObjectType::Attribute)
                                oinfo->num_attrs++;
                        }

                        break;
                    }
                }

                if (!found)
                    throw MetadataError("object_get() info by name did not find matching name");
            }
            else
                throw MetadataError("object_get() unrecognized loc_params->type");

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
    } else                      // passthru
        return VOLBase::object_get(unwrap(mdata_obj), loc_params, args, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
object_specific(void *obj, const H5VL_loc_params_t *loc_params,
        H5VL_object_specific_args_t* args, hid_t dxpl_id, void **req)
{
    auto specific_type = args->op_type;

    auto log = get_logger();
    log->trace("object_specific: specific_type = {}", specific_type);
    Object* obj_ = (Object*) obj;
    if (!unwrap(obj_))
    {
        log->trace("filling token, obj = {}", *obj_);

        switch(specific_type)
        {
            /* H5Oexists_by_name */
        case H5VL_OBJECT_EXISTS: {

            if (obj_->locate(*loc_params).path.empty())
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

            Object* o = obj_->locate(*loc_params).exact();
            log->trace("filling token, {} -> {}", fmt::ptr(o), fmt::ptr((void*)args->args.lookup.token_ptr));
            obj_->fill_token(*args->args.lookup.token_ptr);
            return 0;

            break;
        }
        default:
            throw MetadataError("object_specific(): unrecognized specific_type");
        }
    } else
        return VOLBase::object_specific(unwrap(obj_), loc_params, args, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    auto op_type = args->op_type;

    Object* obj_        = (Object*) obj;

    if (!unwrap(obj_))               // memory
    {
        Object*         mdata_obj   = obj_;

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
        return VOLBase::object_optional(unwrap(obj_), loc_params, args, dxpl_id, req);
}
