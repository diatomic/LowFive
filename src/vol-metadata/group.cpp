#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <cassert>

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto log = get_logger();
    log->trace("group_create:");
    log->trace("loc type = {}, name = {}, obj = {}", loc_params->type, name, *obj_);

    log_assert(obj_->mdata_obj, "mdata_obj not set");

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = wrap(VOLBase::group_create(unwrap(obj_), loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req));
    else
        result = wrap(nullptr);

    // add group to our metadata
    auto obj_path = static_cast<Object*>(obj_->mdata_obj)->search(name);

    // create intermediate groups, if necessary
    do
    {
        auto i = obj_path.path.find('/');
        auto name = obj_path.path.substr(0, i);
        if (name != ".")
        {
            log->trace("Creating group {} under {} with gcpl_id {}", name, obj_path.obj->name, gcpl_id);
            obj_path.obj = obj_path.obj->add_child(new Group(name, gcpl_id));
        } else
            log->trace("Skipping . in the name");
        if (i == std::string::npos)
            obj_path.path = "";
        else
            obj_path.path = obj_path.path.substr(i+1);
        log->trace("Remaining path {}", obj_path.path);
    } while (!obj_path.path.empty());
    result->mdata_obj = obj_path.obj;

    log->trace("group_create: created group = {}", *result);

    return (void*)result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    auto log = get_logger();
    log->trace("group_open: obj = {} name {}", *obj_, name);

    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = wrap(VOLBase::group_open(unwrap(obj_), loc_params, name, gapl_id, dxpl_id, req));
    else
        result = wrap(nullptr);

    // find the group in our file metadata
    std::string name_(name);
    auto obj_path = parent->search(name_);

    if (obj_path.path.empty())
        result->mdata_obj = obj_path.obj;

    if (!result->mdata_obj)
        result->mdata_obj = parent->add_child(new DummyGroup(name_));

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    auto log = get_logger();
    log->trace("MetadataVOL::group_close: {}", *grp_);

    herr_t retval = 0;

    if (unwrap(grp_))
        retval = VOLBase::group_close(unwrap(grp_), dxpl_id, req);

    return retval;
}

herr_t
LowFive::MetadataVOL::
group_optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;
    Object* mdata_obj = static_cast<Object*>(obj_->mdata_obj);

    // native operation's arguments, from H5VLnative_group.c, H5VL__native_group_optional
    auto opt_type = args->op_type;
    H5VL_native_group_optional_args_t *opt_args = static_cast<H5VL_native_group_optional_args_t*>(args->args);

    auto log = get_logger();
    log->trace("group_optional: group = {} optional_type = {}", *obj_, opt_type);

    herr_t res = 0;

    // XXX: this will definitely break if both passthru and memory are on
    if (unwrap(obj_))               // passthru
        res = VOLBase::group_optional(unwrap(obj_), args, dxpl_id, req);
    else if (mdata_obj)             // memory
    {
        // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_GROUP_* constants)
        switch (opt_type)
        {
        // iterate over the objects in a group performing custom callback on each
        case H5VL_NATIVE_GROUP_ITERATE_OLD:
        {
            log->trace("group_optional: opt_type H5VL_NATIVE_GROUP_ITERATE_OLD");

            // resolve location
            H5VL_native_group_iterate_old_t *iter_args = &opt_args->iterate_old;
            mdata_obj = mdata_obj->locate(iter_args->loc_params).exact();

            // get object type in HDF format and use that to get an HDF hid_t to the object
            std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
            int obj_type = h5_types[static_cast<int>(mdata_obj->type)];
            ObjectPointers* obj_tmp = wrap(mdata_obj);
            *obj_tmp = *obj_;
            obj_tmp->tmp = true;
            auto log = get_logger();
            log->trace("group_optional: wrapping {} object type {}", *obj_tmp, mdata_obj->type);
            hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));

            // check direct children of the parent group object, not full search all the way down the tree
            // blindly goes through all the children in the order they were created, ignoring idx

            if (opt_args->iterate_old.idx)
                log->warn("group_optional: for iterating, ignoring the current index which is {}", opt_args->iterate_old.idx);
            else
                log->trace("group_optional: iterating, the current index is unassigned");

            for (auto& c : mdata_obj->children)
            {

                log->trace("group_optional: found object {} as a child of the parent {}", c->name, mdata_obj->name);
                // make the application callback, copied from H5Glink.c, H5G__link_iterate_table()
                res = (opt_args->iterate_old.op)(obj_loc_id, c->name.c_str(), opt_args->iterate_old.op_data);
                if (res > 0)
                {
                    log->trace("group_optional iteration: terminating iteration because operator returned > 0 value, indicating user-defined success and early termination");
                    break;
                }
                else if (res < 0)
                    throw MetadataError(fmt::format("group_optional() opt_type H5VL_NATIVE_GROUP_ITERATE_OLD callback returned nonzero value indicating error"));
            }   // for all children

            auto refcount = H5Idec_ref(obj_loc_id);
            log->trace("refcount = {}", refcount);

            break;
        }
        case H5VL_NATIVE_GROUP_GET_OBJINFO:
        {
            log->trace("group_optional: opt_type H5VL_NATIVE_GROUP_GET_OBJINFO");
            throw MetadataError(fmt::format("group_optional() opt_type H5VL_NATIVE_GROUP_GET_OBJINFO not implemented in metadata yet"));
            break;
        }
        default:
            throw MetadataError(fmt::format("Unknown opt_type in LowFive::MetadataVOL::group_optional()"));
        }
    }
    else
        throw MetadataError(fmt::format("group_optional(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
group_get(void *obj, H5VL_group_get_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto get_type = args->op_type;

    auto log = get_logger();
    log->trace("group_get: group = {}, get_type = {}, req = {}", *obj_, get_type, fmt::ptr(req));

    // enum H5VL_group_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::group_get(unwrap(obj_), args, dxpl_id, req);
    else if (obj_->mdata_obj)
    {                                                       // see hdf5 H5VLnative_group.c, H5VL__native_group_get()
        if (get_type == H5VL_GROUP_GET_GCPL)                // group creation property list
        {
            log->trace("group_get(): get_type H5VL_GROUP_GET_GCPL");

            // check if the object is actually a group
            auto object = static_cast<Object*>(obj_->mdata_obj);
            if (object->type != ObjectType::Group)
            {
                if (object->type == ObjectType::File)
                    args->args.get_gcpl.gcpl_id = H5Pcreate(H5P_GROUP_CREATE);
                else
                    throw MetadataError(fmt::format("group_get(): object type is not a group and not a file"));
            }
            else
            {
                Group* group = static_cast<Group*>(obj_->mdata_obj);
                args->args.get_gcpl.gcpl_id = group->gcpl.id;
                group->gcpl.inc_ref();
            }

            log->trace("arguments = {} -> {}", "args->args.get_gcpl.gcpl_id", args->args.get_gcpl.gcpl_id);
        }
        else if (get_type == H5VL_GROUP_GET_INFO)           // group info
        {
            log->trace("group_get(): get_type H5VL_GROUP_GET_INFO");
            const H5VL_loc_params_t loc_params = args->args.get_info.loc_params;
            H5G_info_t*             group_info = args->args.get_info.ginfo;

            auto* g = static_cast<Object*>(obj_->mdata_obj)->locate(loc_params).exact();
            group_info->storage_type = H5G_STORAGE_TYPE_UNKNOWN;
            group_info->nlinks = g->children.size();
            group_info->max_corder = 0;
            group_info->mounted = false;
        }
        else
            throw MetadataError(fmt::format("group_get() did not recognize get_type = {}", get_type));
    } else
        throw MetadataError(fmt::format("group_get(): either passthru or metadata must be specified"));

    return result;
}

herr_t
LowFive::MetadataVOL::
group_specific(void *obj, H5VL_group_specific_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto specific_type = args->op_type;

    auto log = get_logger();
    log->trace("group_specific: dset = {} specific_type = {}", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::group_specific(unwrap(obj_), args, dxpl_id, req);
    else if (obj_->mdata_obj)
    {
        // specific types are enumerated in H5VLconnector.h
        throw MetadataError(fmt::format("group_specific() not implemented in metadata yet"));
    }
    else
        throw MetadataError(fmt::format("group_specific(): either passthru or metadata must be specified"));

    return res;
}

