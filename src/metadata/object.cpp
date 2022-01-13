#include <lowfive/vol-metadata.hpp>

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
                    // TODO
                    // *ret is a pointer to a H5F_t object
                    // H5_t is defined in H5Fpkg.h, whic is private, so we can't access its fields
//                     H5_t* file = static_cast<H5_t*>(*ret);       // TODO: this won't work
//                     file.open_name = ...
                    throw MetadataError("object_get() file by self not yet implemented");
                }
                else
                    throw MetadataError("Error: object_get() unrecognized loc_params->type\n");

                break;
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
                // oinfo->token = ?                                 // TODO
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
    return VOLBase::object_specific(unwrap(obj), loc_params, specific_type, dxpl_id, req, arguments);
}

