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
group_optional(void *obj, H5VL_group_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("group_optional: group = {} optional_type = {}", *obj_, opt_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::group_optional(unwrap(obj_), opt_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_GROUP_* constants)
        throw MetadataError(fmt::format("group_optional() not implemented in metadata yet"));
    }
    else
        throw MetadataError(fmt::format("group_optional(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("group_get: group = {}, get_type = {}, req = {}", *obj_, get_type, fmt::ptr(req));

    // enum H5VL_group_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(obj_))
        result = VOLBase::group_get(unwrap(obj_), get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {                                                       // see hdf5 H5VLnative_group.c, H5VL__native_group_get()
        va_list args;
        va_copy(args,arguments);

        if (get_type == H5VL_GROUP_GET_GCPL)                // group creation property list
        {
            log->trace("group_get(): get_type H5VL_GROUP_GET_GCPL");
            hid_t *ret = va_arg(arguments, hid_t*);

            // check if the object is actually a group
            auto object = static_cast<Object*>(obj_->mdata_obj);
            if (object->type != ObjectType::Group)
            {
                if (object->type == ObjectType::File)
                    *ret = H5Pcreate(H5P_GROUP_CREATE);
                else
                    throw MetadataError(fmt::format("group_get(): object type is not a group and not a file"));
            }
            else
            {
                Group* group = static_cast<Group*>(obj_->mdata_obj);
                *ret = group->gcpl.id;
                group->gcpl.inc_ref();
            }

            log->trace("arguments = {} -> {}", fmt::ptr(ret), *ret);
        }
        else if (get_type == H5VL_GROUP_GET_INFO)           // group info
        {
            log->trace("group_get(): get_type H5VL_GROUP_GET_INFO");
            const H5VL_loc_params_t *loc_params = va_arg(arguments, const H5VL_loc_params_t *);
            H5G_info_t *             group_info = va_arg(arguments, H5G_info_t *);

            auto* g = static_cast<Object*>(obj_->mdata_obj)->locate(*loc_params).exact();
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
group_specific(void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("group_specific: dset = {} specific_type = {}", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::group_specific(unwrap(obj_), specific_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
    {
        va_list args;
        va_copy(args,arguments);

        // specific types are enumerated in H5VLconnector.h

        throw MetadataError(fmt::format("group_specific() not implemented in metadata yet"));
    }
    else
        throw MetadataError(fmt::format("group_specific(): either passthru or metadata must be specified"));

    return res;
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
