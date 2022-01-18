#include <lowfive/vol-metadata.hpp>
#include <cassert>

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}, mdata = {}\n", loc_params->type, name, obj_->mdata_obj != nullptr);

    assert(obj_->mdata_obj);

    // trace object back to root to build full path and file name
    auto filepath = static_cast<Object*>(obj_->mdata_obj)->fullname(name);

    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = wrap(VOLBase::group_create(unwrap(obj_), loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req));
    else
        result = new ObjectPointers;

    // add group to our metadata
    result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Group(name));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    Object* parent = static_cast<Object*>(obj_->mdata_obj);
    auto filepath = parent->fullname(name);

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    ObjectPointers* result = nullptr;
    if (unwrap(obj_) && match_any(filepath, passthru, true))
        result = wrap(VOLBase::group_open(unwrap(obj_), loc_params, name, gapl_id, dxpl_id, req));
    else
        result = new ObjectPointers;

    // find the group in our file metadata
    std::string name_(name);
    result->mdata_obj = parent->search(name_);
    if (!result->mdata_obj)
        result->mdata_obj = parent->add_child(new DummyGroup(name_));

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = 0;

    if (unwrap(grp_))
        retval = VOLBase::group_close(unwrap(grp_), dxpl_id, req);

    return retval;
}