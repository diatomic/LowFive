#include <lowfive/vol-metadata.hpp>
#include <assert.h>

void *
LowFive::MetadataVOL::
object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    ObjectPointers* result;
    if (!unwrap(obj))           // memory
        result = wrap(nullptr);
    else                        // passthru
        result = wrap(VOLBase::object_open(unwrap(obj), loc_params, opened_type, dxpl_id, req));

    ObjectPointers* obj_        = (ObjectPointers*) obj;
    Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

    // TODO: technically it's even more complicated; it's possible that obj has mdata, but the object we are opening doesn't;
    //       I think locate will return the last parent that has mdata, which is not what we want
    if (mdata_obj)
        result->mdata_obj = mdata_obj->locate(*loc_params).exact();

    return result;
}

herr_t
LowFive::MetadataVOL::
object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    if (!unwrap(src_obj) && !unwrap(dst_obj))   // memory
    {
        ObjectPointers* src_obj_        = (ObjectPointers*) src_obj;
        Object*         src_mdata_obj   = static_cast<Object*>(src_obj_->mdata_obj);
        ObjectPointers* dst_obj_        = (ObjectPointers*) dst_obj;
        Object*         dst_mdata_obj   = static_cast<Object*>(dst_obj_->mdata_obj);

        // TODO
        throw MetadataError(fmt::format("object_copy(): not implemented in metadata yet\n"));
    }
    else                                        // passthru
        return VOLBase::object_copy(unwrap(src_obj), src_loc_params, src_name, unwrap(dst_obj), dst_loc_params, dst_name, ocpypl_id, lcpl_id, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    if (!unwrap(obj))           // look up in memory
    {
        // The following is adapted from HDF5's H5VL__native_object_get() in H5VLnative_object.c

        ObjectPointers* obj_        = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

        // map of our object types to hdf5 object types
        std::vector<int> h5_types = {H5O_TYPE_UNKNOWN, H5O_TYPE_GROUP, H5O_TYPE_DATASET, H5O_TYPE_UNKNOWN, H5O_TYPE_NAMED_DATATYPE};

        switch (get_type)
        {
            case H5VL_OBJECT_GET_FILE:
            {
                fmt::print(stderr, "get_type = H5VL_OBJECT_GET_FILE\n");

                void **ret = va_arg(arguments, void **);
                if (loc_params->type == H5VL_OBJECT_BY_SELF)
                {
                    Object* res = mdata_obj->find_root();
                    assert(res->type == ObjectType::File);
                    ObjectPointers* res_pair = wrap(nullptr);
                    res_pair->mdata_obj = res;
                    res_pair->tmp = true;
                    *ret = res_pair;
                    fmt::print(stderr, "returning {}\n", *res_pair);
                }
                else
                    throw MetadataError("Error: object_get() unrecognized loc_params->type\n");

                return 0;
            }

            case H5VL_OBJECT_GET_NAME:
            {
                fmt::print(stderr, "get_type = H5VL_OBJECT_GET_NAME\n");

                ssize_t *ret    = va_arg(arguments, ssize_t *);
                char *name      = va_arg(arguments, char *);
                size_t size     = va_arg(arguments, size_t);

                if (loc_params->type == H5VL_OBJECT_BY_SELF)
                    strncpy(name, mdata_obj->name.c_str(), size);
                else if (loc_params->type == H5VL_OBJECT_BY_TOKEN)
                {
                    // TODO: ignoring the token and just getting the name of the current object
                    strncpy(name, mdata_obj->name.c_str(), size);
                }
                else
                    throw MetadataError("Error: object_get() unrecognized loc_params->type\n");

                return 0;
            }

            case H5VL_OBJECT_GET_TYPE:
            {
                fmt::print(stderr, "get_type = H5VL_OBJECT_GET_TYPE\n");

                H5O_type_t *obj_type = va_arg(arguments, H5O_type_t *);

                if (loc_params->type == H5VL_OBJECT_BY_TOKEN)
                {
                    // TODO: ignoring the token and just getting the type of the current object
                    int otype   = h5_types[static_cast<int>(mdata_obj->type)];
                    *obj_type   = static_cast<H5O_type_t>(otype);
                }
                else
                    throw MetadataError("Error: object_get() unrecognized loc_params->type\n");

                return 0;
            }

            case H5VL_OBJECT_GET_INFO:
            {
                fmt::print(stderr, "get_type = H5VL_OBJECT_GET_INFO\n");

                H5O_info2_t  *oinfo = va_arg(arguments, H5O_info2_t *); // H5O_info2_t defined in H5Opublic.h
                unsigned fields     = va_arg(arguments, unsigned);

                if (loc_params->type == H5VL_OBJECT_BY_SELF)            // H5Oget_info
                {
                    fmt::print(stderr, "loc_params->type = H5VL_OBJECT_BY_SELF\n");

                    // get object type in HDF format
                    int otype   = h5_types[static_cast<int>(mdata_obj->type)];
                    oinfo->type = static_cast<H5O_type_t>(otype);

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
                    fmt::print(stderr, "loc_params->type = H5VL_OBJECT_BY_NAME\n");

                    // TP: I think this means obj points to the group and we search for the name under direct(?) children of obj?
                    // the name is taken from loc_data.loc_by_name.name
                    bool found = false;
                    for (auto& o : mdata_obj->children)
                    {
                        if (o->name.c_str() == loc_params->loc_data.loc_by_name.name)
                        {
                            found = true;

                            // get object type in HDF format
                            int otype   = h5_types[static_cast<int>(o->type)];
                            oinfo->type = static_cast<H5O_type_t>(otype);

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
                        throw MetadataError("object_get() info by name did not find matching name\n");
                }
                else if (loc_params->type == H5VL_OBJECT_BY_IDX)        // H5Oget_info_by_idx
                {
                    fmt::print(stderr, "loc_params->type = H5VL_OBJECT_BY_IDX\n");

                    // TP: I think this means obj points to the group and we search for the name under direct(?) children of obj?
                    // the name is taken from loc_data.loc_by_idx.name
                    bool found = false;
                    for (auto& o : mdata_obj->children)
                    {
                        if (o->name.c_str() == loc_params->loc_data.loc_by_idx.name)
                        {
                            found = true;

                            // get object type in HDF format
                            int otype   = h5_types[static_cast<int>(o->type)];
                            oinfo->type = static_cast<H5O_type_t>(otype);

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
                        throw MetadataError("object_get() info by name did not find matching name\n");
                }
                else
                    throw MetadataError("Error: object_get() unrecognized loc_params->type\n");

                // TODO: following are uninitialized, unknown, arbitrary
                oinfo->fileno   = 0;                                // TODO
                oinfo->rc       = 0;                                // TODO
                oinfo->atime    = 0;                                // TODO
                oinfo->mtime    = 0;                                // TODO
                oinfo->ctime    = 0;                                // TODO
                oinfo->btime    = 0;                                // TODO

                fmt::print(stderr, "*** ------------------- ***\n");
                fmt::print(stderr, "Warning: getting object info not fully implemented yet.\n");
                fmt::print(stderr, "Ignoring file number, reference counts, times.\n");
                fmt::print(stderr, "*** ------------------- ***\n");

                return 0;
            }

            default:
                throw MetadataError("Error: object_get(): unrecognized get_type");
        }
    } else                      // passthru
        return VOLBase::object_get(unwrap(obj), loc_params, get_type, dxpl_id, req, arguments);
}

herr_t
LowFive::MetadataVOL::
object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    fmt::print(stderr, "object_specific: specific_type = {}\n", specific_type);
    if (!unwrap(obj))
    {
        ObjectPointers* obj_ = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);
        fmt::print(stderr, "filling token, obj = {}\n", *obj_);

        switch(specific_type)
        {
            /* Lookup object */
            case H5VL_OBJECT_LOOKUP: {
                H5O_token_t *token = va_arg(arguments, H5O_token_t *);
                fmt::print(stderr, "loc_params->type = {}\n", loc_params->type);
                fmt::print(stderr, "name = {}\n", loc_params->loc_data.loc_by_name.name);

                // TODO: this really should call mdata_obj->search(...), but that needs to handle '.'
                if (std::string(loc_params->loc_data.loc_by_name.name) == ".")
                {
                    fmt::print(stderr, "filling token, {} -> {}\n", fmt::ptr(mdata_obj), fmt::ptr(token));
                    mdata_obj->fill_token(*token);
                    return 0;
                }

                throw MetadataError("Error: object_specific(): object lookup not implemented");

                break;
            }
            default:
                throw MetadataError("Error: object_specific(): unrecognized specific_type");
        }
    } else
        return VOLBase::object_specific(unwrap(obj), loc_params, specific_type, dxpl_id, req, arguments);
}

herr_t
LowFive::MetadataVOL::
object_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments)
{
    if (!unwrap(obj))               // memory
    {
        ObjectPointers* obj_        = (ObjectPointers*) obj;
        Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

        // see HDF5's H5VL__native_object_optional() in H5VLnative_object.c
        // TODO: not implemented yet
        switch(op_type)
        {
            // H5Oget_comment / H5Oget_comment_by_name
            case H5VL_NATIVE_OBJECT_GET_COMMENT:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_GET_COMMENT not implemented in metadata yet\n"));
            }

            // H5Oset_comment
            case H5VL_NATIVE_OBJECT_SET_COMMENT:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_SET_COMMENT not implemented in metadata yet\n"));
            }

            // H5Odisable_mdc_flushes
            case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES not implemented in metadata yet\n"));
            }

            // H5Oenable_mdc_flushes
            case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES not implemented in metadata yet\n"));
            }

            // H5Oare_mdc_flushes_disabled
            case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED not implemented in metadata yet\n"));
            }

            // H5Oget_native_info(_name|_by_idx)
            case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
            {
                // TODO
                throw MetadataError(fmt::format("object_optional: optional_type H5VL_NATIVE_OBJECT_GET_NATIVE_INFO not implemented in metadata yet\n"));
            }

            default:
                throw MetadataError(fmt::format("object_optional: invalid optional type\n"));
        }

    }
    else                            // passthru
        return VOLBase::object_optional(unwrap(obj), op_type, dxpl_id, req, arguments);
}
