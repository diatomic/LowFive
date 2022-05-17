#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"
#include <cassert>

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    log->trace("group_create:");
    log->trace("loc type = {}, name = {}, obj = {}", loc_params->type, name, *obj_);

    assert(obj_->mdata_obj);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = wrap(VOLBase::group_create(unwrap(obj_), loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req));
    else
        result = wrap(nullptr);

    // add group to our metadata
    auto obj_path = static_cast<Object*>(obj_->mdata_obj)->search(name);
    assert(obj_path.is_name());
    result->mdata_obj = obj_path.obj->add_child(new Group(obj_path.path));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
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

    log->trace("MetadataVOL::group_close: {}", *grp_);

    herr_t retval = 0;

    if (unwrap(grp_))
        retval = VOLBase::group_close(unwrap(grp_), dxpl_id, req);

    return retval;
}
