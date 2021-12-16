#include <lowfive/vol-metadata.hpp>
#include <cassert>

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ptrs = nullptr;

    // create our file metadata; NB: we build the in-memory hierarchy regardless of whether we match memory
    void* mdata = nullptr;
    std::string name_(name);
    File* f = new File(name_);
    files.emplace(name_, f);
    mdata = f;

    if (match_any(name, "", passthru, true))
        obj_ptrs = wrap(VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req));
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    fmt::print("file_create: obj_ptrs = {} [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(obj_ptrs), fmt::ptr(obj_ptrs->h5_obj), fmt::ptr(obj_ptrs->mdata_obj), dxpl_id);

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    fmt::print(stderr, "file_optional: opt_type = {}\n", opt_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    herr_t res = 0;
    if (unwrap(file_))
        res = VOLBase::file_optional(unwrap(file_), opt_type, dxpl_id, req, arguments);

    return res;
}

void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    fmt::print("MetadataVOL::file_open()\n");
    ObjectPointers* obj_ptrs = nullptr;

    // find the file in the VOL
    void* mdata = nullptr;
    std::string name_(name);
    auto it = files.find(name_);
    if (it != files.end())
        mdata = it->second;
    else
    {
        auto* f = new DummyFile(name);
        files.emplace(name, f);
        mdata = f;
    }

    if (match_any(name, "", passthru, true))
        obj_ptrs = wrap(VOLBase::file_open(name, flags, fapl_id, dxpl_id, req));
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    fmt::print("Opened file {}: {} {}\n", name, fmt::ptr(obj_ptrs->mdata_obj), fmt::ptr(obj_ptrs->h5_obj));

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    va_list args;
    va_copy(args,arguments);

    fmt::print("file_get: file = {} [h5_obj {} mdata_obj {}], get_type = {} req = {} dxpl_id = {}\n",
            fmt::ptr(file_), fmt::ptr(file_->h5_obj), fmt::ptr(file_->mdata_obj), get_type, fmt::ptr(req), dxpl_id);
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(file_))
        result = VOLBase::file_get(unwrap(file_), get_type, dxpl_id, req, arguments);
    // else: TODO
    else
        fmt::print("Warning: requested file_get() not implemented for in-memory regime\n");

    return result;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    herr_t res = 0;

    if (unwrap(file_))
        res = VOLBase::file_close(unwrap(file_), dxpl_id, req);

    // TODO: add DummyFile condition
    if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        // we created this file
        files.erase(f->name);
        delete f;       // erases all the children too
    }

    // deliberately verbose, to emphasize checking of res
    if (res == 0)
        return 0;
    else
        return res;
}

herr_t
LowFive::MetadataVOL::
file_specific(void *file, H5VL_file_specific_t specific_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    return VOLBase::file_specific(unwrap(file), specific_type, dxpl_id, req, arguments);
}
