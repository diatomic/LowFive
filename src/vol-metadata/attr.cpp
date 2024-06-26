#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
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

    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Attribute(name, type_id, space_id));
    log->trace("created attribute named {} in metadata, new object {} under parent object {} named {}",
            name, *result, obj_->mdata_obj, static_cast<Object*>(obj_->mdata_obj)->name);

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

    if (unwrap(obj_))
    {
        auto* under_result = VOLBase::attr_open(unwrap(obj_), loc_params, name, aapl_id, dxpl_id, req);
        if (!under_result)      // not found in passthru, return nullptr; NB: if it somehow exists in metadata, this is problematic
            return under_result;
        result = wrap(under_result);
    }
    else
        result = wrap(nullptr);

    auto* mdata_obj = static_cast<Object*>(obj_->mdata_obj);
    if (mdata_obj)
    {
        // trace object back to root to build full path and file name
        auto filepath = mdata_obj->fullname(name);

        // debug
        log->trace("attr_open filepath first {} second {} name {}", filepath.first, filepath.second, name);

        // find the attribute in our file metadata
        std::string name_(name);
        if (match_any(filepath, memory))
        {
            auto op = mdata_obj->locate(*loc_params).exact()->search(name_);
            if (!op.path.empty())
                return nullptr;     // hopefully this returns -1 from the calling function

            result->mdata_obj = mdata_obj->locate(*loc_params).exact()->search(name_).exact();
        }
        // TODO: make a dummy attribute if not found; this will be triggered by assertion failure in exact
    }

    log->trace("attr_open search result = {} = [h5_obj {} mdata_obj {}] name {}",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
attr_read(void *attr, hid_t mem_type_id, void *buf,
        hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    auto log = get_logger();
    log->trace("Attr Read");
    auto mem_type = Datatype(mem_type_id);
    log->trace("attr = {}, mem_type_id = {}, mem type = {}", *attr_, mem_type_id, mem_type);

    if (unwrap(attr_))
        return VOLBase::attr_read(unwrap(attr), mem_type_id, buf, dxpl_id, req);
    else
    {
        log_assert(attr_->mdata_obj, "mdata_obj must be set in metadata mode");
        Attribute* a = (Attribute*) attr_->mdata_obj;

        resolve_references(a);

        log->trace("type = {}, space = {}, mem_type = {}", a->type, a->space, a->mem_type);
        a->read(Datatype(mem_type), buf);
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto get_type = args->op_type;

    auto log = get_logger();
    log->trace("attr = {}, get_type = {}, req = {}", *obj_, get_type, fmt::ptr(req));

    log->trace("Attr Get");
    log->trace("get type = {}", get_type);

    // TODO: again, why do we prefer passthru?
    if (unwrap(obj_))
        return VOLBase::attr_get(unwrap(obj), args, dxpl_id, req);
    else if (obj_->mdata_obj)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            args->args.get_space.space_id = space_id;
            log->trace("arguments = {} -> {}", "args->args.get_space.space_id", args->args.get_space.space_id);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            args->args.get_type.type_id = dtype_id;
            log->trace("arguments = {} -> {}", "args->args.get_type.type_id", args->args.get_type.type_id);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in attr_get()", get_type));
        }
    }

    return 0;
}

// helper function for attr_specific()
htri_t
LowFive::MetadataVOL::
attr_exists(Object *mdata_obj, const char* attr_name, htri_t* ret)
{
    // check direct children of the parent object (NB, not full search all the way down the tree)
    *ret = 0;           // not found
    for (auto& c : mdata_obj->children)
    {
        if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name)
        {
            *ret = 1;   // found
            break;
        }
    }
    auto log = get_logger();
    if (*ret)
        log->trace("Found attribute {} as a child of the parent {}", attr_name, mdata_obj->name);
    else
        log->trace("Did not find attribute {} as a child of the parent {}", attr_name, mdata_obj->name);

    return *ret;
}

herr_t
LowFive::MetadataVOL::
attr_iter(void *obj, const H5VL_loc_params_t *loc_params, H5_iter_order_t order, hsize_t *idx, H5A_operator2_t op, void* op_data)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;
    Object* mdata_obj = static_cast<Object*>(obj_->mdata_obj);

    mdata_obj = mdata_obj->locate(*loc_params).exact();

    // get object type in HDF format and use that to get an HDF hid_t to the object
    std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
    int obj_type = h5_types[static_cast<int>(mdata_obj->type)];
    ObjectPointers* obj_tmp = wrap(nullptr);
    *obj_tmp = *obj_;
    obj_tmp->tmp = true;
    auto log = get_logger();
    log->trace("attr_iter: wrapping {} object type {}", *obj_tmp, mdata_obj->type);

    hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));
    //log->trace("wrap_object = {}", fmt::ptr(H5VLobject(obj_loc_id)));
    //log->trace("dec_ref, refcount = {}", H5Idec_ref(obj_loc_id));

    // info for attribute, defined in HDF5 H5Apublic.h  TODO: assigned with some defaults, not sure if they're corrent
    H5A_info_t ainfo;
    ainfo.corder_valid  = true;                     // whether creation order is valid
    ainfo.corder        = 0;                        // creation order (TODO: no idea what this is)
    ainfo.cset          = H5T_CSET_ASCII;           // character set of attribute names

    // check direct children of the parent object, not full search all the way down the tree
    bool found      = false;                        // some attributes were found
    herr_t retval   = 0;                            // return value

    // TODO: currently ignores the iteration order, increment direction, and current index
    // just blindly goes through all the attributes in the order they were created
    for (auto& c : mdata_obj->children)
    {
        if (c->type == LowFive::ObjectType::Attribute)
        {
            ainfo.data_size =                               // size of raw data (bytes)
                    static_cast<Attribute*>(c)->space.size() * static_cast<Attribute*>(c)->type.dtype_size;
            found = true;
            log->trace("attr_iter: found attribute {} with data_size {} as a child of the parent {}", c->name, ainfo.data_size, mdata_obj->name);
            if (idx)
                log->trace("attr_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is {}", order, *idx);
            else
                log->trace("attr_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is unassigned", order);

            std::vector<char> c_name(c->name.size() + 1);
            strcpy(c_name.data(), c->name.c_str());

            // make the application callback, copied from H5Aint.c, H5A__attr_iterate_table()
            retval = (op)(obj_loc_id, c_name.data(), &ainfo, op_data);
            if (retval > 0)
            {
                log->trace("attr_iter: terminating iteration because operator returned > 0 value, indicating user-defined success and early termination");
                break;
            }
            else if (retval < 0)
            {
                log->trace("attr_iter: terminating iteration because operator returned < 0 value, indicating user-defined failure and early termination");
                break;
            }
        }   // child is type attribute

        // we should be incrementing idx, but it's more nuanced; in the native VOL, on return it's set to the last accessed attribute index
        //++(*idx);

    }   // for all children

    auto refcount = H5Idec_ref(obj_loc_id);
    log->trace("refcount = {}", refcount);
    // NB: don't need to delete obj_tmp; it gets deleted (via
    //     MetadataVOL::drop()) automagically, when refcount reaches 0,
    //     i.e., this part works as expected

    if (!found)
    {
        log->trace("attr_iter: did not find any attributes as direct children of the parent {} when trying to iterate over attributes", mdata_obj->name);
    }

    return retval;
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
        log->trace("type = {}, space = {}, mem_type = {}", a->type, a->space, a->mem_type);
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

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto specific_type = args->op_type;

    auto* mdata_obj = static_cast<Object*>(obj_->mdata_obj);

    auto log = get_logger();
    log->trace("attr_specific obj = {} specific_type = {}", *obj_, specific_type);
    log->trace("specific types H5VL_ATTR_DELETE = {} H5VL_ATTR_EXISTS = {} H5VL_ATTR_ITER = {} H5VL_ATTR_RENAME = {}",
            H5VL_ATTR_DELETE, H5VL_ATTR_EXISTS, H5VL_ATTR_ITER, H5VL_ATTR_RENAME);

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::attr_specific(unwrap(obj_), loc_params, args, dxpl_id, req);

    else // if (match_any(filepath, memory))
    {
        switch(specific_type)
        {
        case H5VL_ATTR_DELETE:                      // H5Adelete(_by_name/idx)
        {
            Attribute* attr = nullptr;

            if (H5VL_OBJECT_BY_SELF == loc_params->type)
            {
                // we still search by name, but only in immediate children
                // mdata_obj is dataset/group, not the attribute to be deleted
                const char *attr_name_ = args->args.del.name;
                std::string attr_name(attr_name_);
                log->trace("attr_specific: specific type H5VL_ATTR_DELETE locate object by self, attr_name = {}, mdata_obj = {}", attr_name, fmt::ptr(mdata_obj));
                bool found = false;
                for (auto& c : mdata_obj->children)
                {
                    if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name_)
                    {
                        attr = dynamic_cast<Attribute*>(c);
                        found = true;
                        break;
                    }
                }
                log->trace("attr_specific: attr = {}", fmt::ptr(attr));
                if (!found)
                    throw MetadataError(fmt::format("MetadataVOL::attr_specific: Did not find attribute {} as a child of the parent {}", attr_name, mdata_obj->name));
            }
            else if (H5VL_OBJECT_BY_NAME == loc_params->type)
            {
                const char *attr_name = args->args.del.name;
                Object* o = mdata_obj->locate(*loc_params).exact();
                attr = dynamic_cast<Attribute*>(o->search(attr_name).exact());
                log->trace("attr_specific: specific type H5VL_ATTR_DELETE locate object by name, attr_name = {}, mdata_obj = {}, attr = {}", attr_name, fmt::ptr(mdata_obj), fmt::ptr(attr));
            }
            else if (H5VL_OBJECT_BY_IDX == loc_params->type)
            {
                // need to use: args->args.delete_by_idx.{order,n,idx_type}
                throw MetadataError(fmt::format("H5VL_ATTR_DELETE locate object by index not yet implemented in LowFive::MetadataVOL::attr_specific()"));
            }

            // remove attribute from metadata tree and free its memory
            // will throw, if dynamic cast failed
            attr->remove();
            delete attr;
            break;
        }
        case H5VL_ATTR_EXISTS:                      // H5Aexists(_by_name)
        {
            log->trace("attr_specific: specific_type H5VL_ATTR_EXISTS");
            htri_t ret;
            result = attr_exists(mdata_obj->locate(*loc_params).exact(), args->args.exists.name, &ret);
            *args->args.exists.exists = (ret == 1);

            break;
        }
        case H5VL_ATTR_ITER:                        // H5Aiterate(_by_name)
        {
            log->trace("attr_specific: specific_type H5VL_ATTR_ITER");

            // sanity check that the provided object matches the location parameters
            // ie,  we're not supposed to operate on one of the children instead of the parent (which we don't support)
            if (mdata_obj != mdata_obj->locate(*loc_params).exact())
                throw MetadataError(fmt::format("attr__specific: specific_type H5VL_LINK_ITER, object does not match location parameters"));

            result = attr_iter(obj, loc_params, args->args.iterate.order, args->args.iterate.idx, args->args.iterate.op, args->args.iterate.op_data);

            break;
        }
        case H5VL_ATTR_RENAME:                     // H5Arename(_by_name)
        {
            const char *old_name = args->args.rename.old_name;
            const char *new_name = args->args.rename.new_name;
            log->trace("attr_specific: specific_type H5VL_ATTR_RENAME: old_name = {}, new_name = {}, loc_params->type = {}, loc_params->name = {}",
                    old_name, new_name, loc_params->type, loc_params->loc_data.loc_by_name.name);

            if (loc_params->type == H5VL_OBJECT_BY_SELF) { /* H5Arename */
                auto* attr = static_cast<Attribute*>(mdata_obj);
                attr->name = new_name;
            } else if (loc_params->type == H5VL_OBJECT_BY_NAME) { /* H5Arename_by_name */
                Object* o = mdata_obj->locate(*loc_params).exact();
                Object* attr = o->search(old_name).exact();
                attr->name = new_name;
            } else
                throw MetadataError(fmt::format("attr_specific: unknown loc_params type for specific_type H5VL_ATTR_RENAME"));
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
attr_optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    return VOLBase::attr_optional(unwrap(obj), args, dxpl_id, req);
}

