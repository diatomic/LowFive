#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"
#include <cassert>

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto log = get_logger();
    log->trace("Attr Create");
    log->trace("loc type = {}, name = {}", loc_params->type, name);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath,passthru))
    {
        result = wrap(VOLBase::attr_create(unwrap(obj_), loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req));
        log->trace("created attribute named {} in passthru object {}", name, *result);
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
            log->trace("attribute name {} exists already in metadata object {}", name, *result);
        }
        else
        {
            result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Attribute(name, type_id, space_id));
            log->trace("created attribute named {} in metadata, new object {} under parent object {} named {}",
                    name, *result, obj_->mdata_obj, static_cast<Object*>(obj_->mdata_obj)->name);
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

    auto log = get_logger();
    log->trace("Attr Open");
    log->trace("attr_open obj = {} name {}", *obj_, name);

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

    log->trace("attr_open search result = {} = [h5_obj {} mdata_obj {}] name {}",
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

    auto log = get_logger();
    log->trace("attr = {}, get_type = {}, req = {}", *obj_, get_type, fmt::ptr(req));

    log->trace("Attr Get");
    log->trace("get type = {}", get_type);

    // TODO: again, why do we prefer passthru?
    if (unwrap(obj_))
        return VOLBase::attr_get(unwrap(obj), get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in attr_get()", get_type));
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
    auto log = get_logger();
    if (*ret)
        log->trace("Found attribute {} as a child of the parent {}", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
    else
        log->trace("Did not find attribute {} as a child of the parent {}", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
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
    auto log = get_logger();
    log->trace("wrapping {}", *obj_tmp);

    hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));
    //log->trace("wrap_object = {}", fmt::ptr(H5VLobject(obj_loc_id)));
    //log->trace("dec_ref, refcount = {}", H5Idec_ref(obj_loc_id));

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

            log->trace("Found attribute {} as a child of the parent {}", c->name, mdata_obj->name);

            log->trace("*** ------------------- ***");
            log->trace("Warning: operating on attribute not fully implemented yet.");
            log->trace("Ignoring attribute info, attribute order, increment direction, current index.");
            log->trace("Stepping through all attributes of the object in the order they were created.");
            if (idx)
                log->trace("The provided order (H5_iter_order_t in H5public.h) is {} and the current index is {}", order, *idx);
            else
                log->trace("The provided order (H5_iter_order_t in H5public.h) is {} and the current index is unassigned", order);
            log->trace("*** ------------------- ***");

            // make the application callback, copied from H5Aint.c, H5A__attr_iterate_table()
            herr_t retval = (op)(obj_loc_id, c->name.c_str(), &ainfo, op_data);
            if (retval > 0)
            {
                log->trace("Terminating iteration because operator returned > 0 value, indicating user-defined early termination");
                break;
            }
            else if (retval < 0)
            {
                log->trace("Terminating iteration because operator returned < 0 value, indicating user-defined failure");
                break;
            }
        }   // child is type attribute
    }   // for all children

    log->trace("refcount = {}", H5Idec_ref(obj_loc_id));
    // NB: don't need to delete obj_tmp; it gets deleted (via
    //     MetadataVOL::drop()) automagically, when refcount reaches 0,
    //     i.e., this part works as expected

    if (!found)
    {
        log->trace("Did not find any attributes as direct children of the parent {} when trying to iterate over attributes\n.", mdata_obj->name);
    }
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    auto log = get_logger();
    log->trace("attr_specific obj = {} specific_type = {}", *obj_, specific_type);
    log->trace("specific types H5VL_ATTR_DELETE = {} H5VL_ATTR_EXISTS = {} H5VL_ATTR_ITER = {} H5VL_ATTR_RENAME = {}",
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
                throw MetadataError(fmt::format("H5VL_ATTR_DELETE not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            case H5VL_ATTR_EXISTS:                      // H5Aexists(_by_name)
            {
                log->trace("case H5VL_ATTR_EXISTS");
                attr_exists(obj, arguments);

                break;
            }
            case H5VL_ATTR_ITER:                        // H5Aiterate(_by_name)
            {
                log->trace("case H5VL_ATTR_ITER");
                attr_iter(obj, arguments);

                break;
            }
            case H5VL_ATTR_RENAME:                     // H5Arename(_by_name)
            {
                // TODO
                throw MetadataError(fmt::format("H5VL_ATTR_RENAME not yet implemented in LowFive::MetadataVOL::attr_specific()"));
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

    auto log = get_logger();
    log->trace("Attr Write");
    log->trace("attr = {}, mem_type_id = {}, mem type = {}",
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

    auto log = get_logger();
    log->trace("Attr Close");
    log->trace("close: {}, dxpl_id = {}", *attr_, dxpl_id);

    herr_t retval = 0;

    if (unwrap(attr_))
        retval = VOLBase::attr_close(unwrap(attr_), dxpl_id, req);

    return retval;
}
