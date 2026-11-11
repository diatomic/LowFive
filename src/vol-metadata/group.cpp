#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <cassert>

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    auto* parent_pointers = static_cast<ObjectPointers*>(obj);
    if (!parent_pointers->writable()) return nullptr;
    auto* parent = static_cast<Object*>(static_cast<ObjectPointers*>(obj)->mdata_obj);
    auto filepath = parent->fullname(name);
    const bool native = unwrap(obj) && match_any(filepath, passthru, true);
    auto path = metadata_creation_path(parent, name, native);
    if (!path.obj || path.path.empty()) return nullptr;
    if (!native && !metadata_creation_allowed(path, lcpl_id)) return nullptr;
    void* underlying = nullptr;
    if (native)
    {
        underlying = VOLBase::group_create(unwrap(obj), loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    path = metadata_create_parents(*this, std::move(path), parent_pointers,
                                   native, dxpl_id);
    if (!path.obj)
    {
        if (underlying) VOLBase::group_close(underlying, dxpl_id, nullptr);
        return nullptr;
    }
    auto* metadata = path.obj->add_child(new Group(path.path, gcpl_id));
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->inherit_file_state(*parent_pointers);
    return result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    auto* parent = static_cast<Object*>(static_cast<ObjectPointers*>(obj)->mdata_obj);
    auto path = parent->search(name);
    void* underlying = nullptr;
    if (unwrap(obj) && match_any(parent->fullname(name), passthru, true))
    {
        underlying = VOLBase::group_open(unwrap(obj), loc_params, name, gapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    Object* metadata = path.path.empty() ? path.obj : nullptr;
    if (metadata && metadata->type != ObjectType::Group && metadata->type != ObjectType::File)
    {
        if (underlying) VOLBase::group_close(underlying, dxpl_id, req);
        return nullptr;
    }
    if (!metadata)
    {
        if (!underlying) return nullptr;
        H5VL_group_get_args_t get {};
        get.op_type = H5VL_GROUP_GET_GCPL;
        if (VOLBase::group_get(underlying, &get, dxpl_id, nullptr) < 0)
        {
            VOLBase::group_close(underlying, dxpl_id, nullptr);
            return nullptr;
        }
        Hid gcpl(get.args.get_gcpl.gcpl_id, true);
        auto target = metadata_target_path(parent, name);
        metadata = target.obj && !target.path.empty() ?
            target.obj->add_path(new Group("", gcpl.id), target.path) : nullptr;
        if (!metadata)
        {
            VOLBase::group_close(underlying, dxpl_id, nullptr);
            return nullptr;
        }
    }
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->inherit_file_state(*static_cast<ObjectPointers*>(obj));
    return result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;
    if (grp_->tmp) return 0;

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
    {
        // warn if metadata are available but won't be used
        if (mdata_obj)
            log->warn("Warning: group_optional: obj = {} metadata are available but won't be used", *obj_);

        res = VOLBase::group_optional(unwrap(obj_), args, dxpl_id, req);
    }
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
            if (!mdata_obj ||
                (mdata_obj->type != ObjectType::File &&
                 mdata_obj->type != ObjectType::Group)) return -1;

            const auto obj_type = get_identifier_type(mdata_obj);
            ObjectPointers* obj_tmp = wrap(mdata_obj);
            *obj_tmp = *obj_;
            obj_tmp->mdata_obj = mdata_obj;
            obj_tmp->tmp = true;
            auto log = get_logger();
            log->trace("group_optional: wrapping {} object type {}", *obj_tmp, mdata_obj->type);
            hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));
            if (obj_loc_id < 0) return -1;

            auto children = mdata_obj->links();
            const hsize_t start = iter_args->idx;
            if (start > children.size())
            {
                H5Idec_ref(obj_loc_id);
                return -1;
            }
            if (iter_args->last_obj) *iter_args->last_obj = start;
            size_t position = 0;
            for (auto* c : children)
            {
                if (position++ < static_cast<size_t>(start)) continue;

                log->trace("group_optional: found object {} as a child of the parent {}", c->name, mdata_obj->name);

                std::vector<char> c_name(c->name.size() + 1);
                strcpy(c_name.data(), c->name.c_str());

                // make the application callback, copied from H5Glink.c, H5G__link_iterate_table()
                res = (opt_args->iterate_old.op)(obj_loc_id, c_name.data(), opt_args->iterate_old.op_data);
                if (iter_args->last_obj) *iter_args->last_obj = position;
                if (res > 0)
                {
                    log->trace("group_optional iteration: terminating iteration because operator returned > 0 value, indicating user-defined success and early termination");
                    break;
                }
                else if (res < 0)
                    break;
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
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: group_get: obj = {} metadata are available but won't be used", *obj_);

        result = VOLBase::group_get(unwrap(obj_), args, dxpl_id, req);
    }
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
                args->args.get_gcpl.gcpl_id = H5Pcopy(group->gcpl.id);
            }

            log->trace("arguments = {} -> {}", "args->args.get_gcpl.gcpl_id", args->args.get_gcpl.gcpl_id);
        }
        else if (get_type == H5VL_GROUP_GET_INFO)           // group info
        {
            log->trace("group_get(): get_type H5VL_GROUP_GET_INFO");
            const H5VL_loc_params_t loc_params = args->args.get_info.loc_params;
            H5G_info_t*             group_info = args->args.get_info.ginfo;

            auto* g = static_cast<Object*>(obj_->mdata_obj)->locate(loc_params).exact();
            if (!g || (g->type != ObjectType::File &&
                       g->type != ObjectType::Group)) return -1;
            group_info->storage_type = H5G_STORAGE_TYPE_UNKNOWN;
            group_info->nlinks = g->links().size();
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
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: group_specific: obj = {} metadata are available but won't be used", *obj_);

        res = VOLBase::group_specific(unwrap(obj_), args, dxpl_id, req);
    }
    else if (obj_->mdata_obj)
    {
        // specific types are enumerated in H5VLconnector.h
        throw MetadataError(fmt::format("group_specific() not implemented in metadata yet"));
    }
    else
        throw MetadataError(fmt::format("group_specific(): either passthru or metadata must be specified"));

    return res;
}

