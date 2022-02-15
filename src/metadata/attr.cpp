#include <lowfive/vol-metadata.hpp>
#include <cassert>

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Attr Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath,passthru))
    {
        result = wrap(VOLBase::attr_create(unwrap(obj_), loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req));
        fmt::print(stderr, "created attribute name {} in passthru object {}\n", name, *result);
    }
    else
        result = wrap(nullptr);

    // add attribute to our metadata; NB: attribute cannot have children, so only creating it if we have to
    if (match_any(filepath,memory))
    {
        // check if the attribute exists already
        bool found = false;
        for (auto& c : static_cast<Object*>(obj_->mdata_obj)->children)
        {
            if (c->type == LowFive::ObjectType::Attribute && c->name == name)
            {
                found = true;
                result->mdata_obj = c;
                break;
            }
        }
        if (found)
        {
            fmt::print(stderr, "attribute name {} exists already in metadata object {}\n", name, *result);
        }
        else
        {
            result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Attribute(name, type_id, space_id));
            fmt::print(stderr, "created attribute name {} in metadata object {}\n", name, *result);
        }
    }

    return result;
}

void*
LowFive::MetadataVOL::
attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "Attr Open\n");
    fmt::print("attr_open obj = {} name {}\n", *obj_, name);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    if (match_any(filepath, passthru))
        result = wrap(VOLBase::attr_open(unwrap(obj_), loc_params, name, aapl_id, dxpl_id, req));
    else
        result = wrap(nullptr);

    // find the attribute in our file metadata
    std::string name_(name);
    if (match_any(filepath, memory))
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_).exact();
    // TODO: make a dummy attribute if not found; this will be triggered by assertion failure in exact

    fmt::print("attr_open search result = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    return VOLBase::attr_read(unwrap(attr), mem_type_id, buf, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr = {}, get_type = {}, req = {}\n", *obj_, get_type, fmt::ptr(req));

    fmt::print(stderr, "Attr Get\n");
    fmt::print("get type = {}\n", get_type);

    // TODO: again, why do we prefer passthru?
    if (unwrap(obj_))
        return VOLBase::attr_get(unwrap(obj), get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

            fmt::print("dataset data type id = {}, datatype = {}\n",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            fmt::print("copied data type id = {}, datatype = {}\n",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Warning, unknown get_type == {} in attr_get()", get_type));
        }
    }

    return 0;
}

// helper function for attr_specific()
void
LowFive::MetadataVOL::
attr_exists(void *obj, va_list arguments)
{
    ObjectPointers* obj_    = (ObjectPointers*) obj;

    const char *attr_name   = va_arg(arguments, const char *);
    htri_t *    ret         = va_arg(arguments, htri_t *);

    // check direct children of the parent object (NB, not full search all the way down the tree)
    *ret = 0;           // not found
    for (auto& c : static_cast<Object*>(obj_->mdata_obj)->children)
    {
        if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name)
        {
            *ret = 1;   // found
            break;
        }
    }
    if (*ret)
        fmt::print(stderr, "Found attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
    else
        fmt::print(stderr, "Did not find attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
}

// helper function for attr_specific()
void
LowFive::MetadataVOL::
attr_iter(void *obj, va_list arguments)
{
    ObjectPointers* obj_        = (ObjectPointers*) obj;
    Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

    // copied from HDF5 H5VLnative_attr.c, H5VL__native_attr_specific()
    H5_index_t      idx_type = (H5_index_t)va_arg(arguments, int);
    H5_iter_order_t order    = (H5_iter_order_t)va_arg(arguments, int);
    hsize_t *       idx      = va_arg(arguments, hsize_t *);
    H5A_operator2_t op       = va_arg(arguments, H5A_operator2_t);
    void *          op_data  = va_arg(arguments, void *);

    // get object type in HDF format and use that to get an HDF hid_t to the object
    std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
    int obj_type = h5_types[static_cast<int>(mdata_obj->type)];
    ObjectPointers* obj_tmp = wrap(nullptr);
    *obj_tmp = *obj_;
    obj_tmp->tmp = true;
    fmt::print(stderr, "wrapping {}\n", *obj_tmp);

    hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));
    //fmt::print(stderr, "wrap_object = {}\n", fmt::ptr(H5VLobject(obj_loc_id)));
    //fmt::print(stderr, "dec_ref, refcount = {}", H5Idec_ref(obj_loc_id));

    // info for attribute, defined in HDF5 H5Apublic.h  TODO: assigned with some defaults, not sure if they're corrent
    H5A_info_t ainfo;
    ainfo.corder_valid  = true;                     // whether creation order is valid
    ainfo.corder        = 0;                        // creation order (TODO: no idea what this is)
    ainfo.cset          = H5T_CSET_ASCII;           // character set of attribute names
    ainfo.data_size =                               // size of raw data (bytes)
        static_cast<Attribute*>(mdata_obj)->space.size() * static_cast<Attribute*>(mdata_obj)->type.dtype_size;

    // check direct children of the parent object, not full search all the way down the tree
    bool found = false;         // some attributes were found

    // TODO: currently ignores the iteration order, increment direction, and current index
    // just blindly goes through all the attributes in the order they were created
    for (auto& c : mdata_obj->children)
    {
        if (c->type == LowFive::ObjectType::Attribute)
        {
            found = true;

            fmt::print(stderr, "Found attribute {} as a child of the parent {}\n", c->name, mdata_obj->name);

            fmt::print(stderr, "*** ------------------- ***\n");
            fmt::print(stderr, "Warning: operating on attribute not fully implemented yet.\n");
            fmt::print(stderr, "Ignoring attribute info, attribute order, increment direction, current index.\n");
            fmt::print(stderr, "Stepping through all attributes of the object in the order they were created.\n");
            if (idx)
                fmt::print(stderr, "The provided order (H5_iter_order_t in H5public.h) is {} and the current index is {}\n", order, *idx);
            else
                fmt::print(stderr, "The provided order (H5_iter_order_t in H5public.h) is {} and the current index is unassigned\n", order);
            fmt::print(stderr, "*** ------------------- ***\n");

            // make the application callback, copied from H5Aint.c, H5A__attr_iterate_table()
            herr_t retval = (op)(obj_loc_id, c->name.c_str(), &ainfo, op_data);
            if (retval > 0)
            {
                fmt::print(stderr, "Terminating iteration because operator returned > 0 value, indicating user-defined early termination\n");
                break;
            }
            else if (retval < 0)
            {
                fmt::print(stderr, "Terminating iteration because operator returned < 0 value, indicating user-defined failure\n");
                break;
            }
        }   // child is type attribute
    }   // for all children

    fmt::print(stderr, "refcount = {}\n", H5Idec_ref(obj_loc_id));
    // NB: don't need to delete obj_tmp; it gets deleted (via
    //     MetadataVOL::drop()) automagically, when refcount reaches 0,
    //     i.e., this part works as expected

    if (!found)
    {
        fmt::print(stderr, "Did not find any attributes as direct children of the parent {} when trying to iterate over attributes\n.", mdata_obj->name);
    }
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr_specific obj = {} specific_type = {}\n",
            *obj_, specific_type);
    fmt::print("specific types H5VL_ATTR_DELETE = {} H5VL_ATTR_EXISTS = {} H5VL_ATTR_ITER = {} H5VL_ATTR_RENAME = {}\n",
            H5VL_ATTR_DELETE, H5VL_ATTR_EXISTS, H5VL_ATTR_ITER, H5VL_ATTR_RENAME);

    // trace object back to root to build full path and file name
    auto name = static_cast<Object*>(obj_->mdata_obj)->name;
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::attr_specific(unwrap(obj_), loc_params, specific_type, dxpl_id, req, arguments);

    else if (match_any(filepath, memory))
    {
        switch(specific_type)
        {
            case H5VL_ATTR_DELETE:                      // H5Adelete(_by_name/idx)
            {
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_DELETE not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            case H5VL_ATTR_EXISTS:                      // H5Aexists(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_EXISTS\n");
                attr_exists(obj, arguments);

                break;
            }
            case H5VL_ATTR_ITER:                        // H5Aiterate(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_ITER\n");
                attr_iter(obj, arguments);

                break;
            }
            case H5VL_ATTR_RENAME:                     // H5Arename(_by_name)
            {
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_RENAME not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            default:
                throw MetadataError(fmt::format("Unknown specific_type in LowFive::MetadataVOL::attr_specific()"));
        }
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req,
    va_list arguments)
{
    return VOLBase::attr_optional(unwrap(obj), opt_type, dxpl_id, req, arguments);
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print(stderr, "Attr Write\n");
    fmt::print(stderr, "attr = {}, mem_type_id = {}, mem type = {}\n",
            *attr_, mem_type_id, Datatype(mem_type_id));

    if (attr_->mdata_obj)
    {
        Attribute* a = (Attribute*) attr_->mdata_obj;
        a->write(Datatype(mem_type_id), buf);
    }

    if (unwrap(attr_))
        return VOLBase::attr_write(unwrap(attr_), mem_type_id, buf, dxpl_id, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print(stderr, "Attr Close\n");
    fmt::print(stderr, "close: {}, dxpl_id = {}\n", *attr_, dxpl_id);

    herr_t retval = 0;

    if (unwrap(attr_))
        retval = VOLBase::attr_close(unwrap(attr_), dxpl_id, req);

    return retval;
}
