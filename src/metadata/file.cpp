#include <lowfive/vol-metadata.hpp>
#include <cassert>
#include "../log-private.hpp"

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("enter MetadataVOL::file_create, name ={}", name);
    ObjectPointers* obj_ptrs = nullptr;

    // create our file metadata; NB: we build the in-memory hierarchy regardless of whether we match memory
    void* mdata = nullptr;
    std::string name_(name);
    File* f = new File(name_, fcpl_id, fapl_id);
    files.emplace(name_, f);
    mdata = f;

    log->trace("MetadataVOL::file_create, name ={}, created File f= {}", name, fmt::ptr(f));

    if (match_any(name, "", passthru, true)) {
        log->trace("MetadataVOL::file_create, name ={}, calling VOLBase for passthru", name);
        obj_ptrs = wrap(VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req));
    } else {
        log->trace("MetadataVOL::file_create, name ={}, wrapping nullptr", name);
        obj_ptrs = wrap(nullptr);
    }

    obj_ptrs->mdata_obj = mdata;

    log->trace("file_create: obj_ptr = {}, dxpl_id = {}", *obj_ptrs, dxpl_id);

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    auto log = get_logger();
    log->trace("file_optional: file = {}, opt_type = {}", *file_, opt_type);
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
    auto log = get_logger();
    log->trace("file_open()");
    ObjectPointers* obj_ptrs = nullptr;

    // find the file in the VOL
    void* mdata = nullptr;
    std::string name_(name);
    auto it = files.find(name_);
    if (it != files.end())
    {
        log->trace("Found file {}", name_);
        mdata = it->second;
    }
    else
    {
        log->trace("Didn't find file {}, creating DummyFile", name_);
        auto* f = new DummyFile(name);
        files.emplace(name, f);
        mdata = f;
    }

    if (match_any(name, "", passthru, true))
        obj_ptrs = wrap(VOLBase::file_open(name, flags, fapl_id, dxpl_id, req));
    else
        obj_ptrs = wrap(nullptr);

    obj_ptrs->mdata_obj = mdata;

    log->trace("Opened file {}: {}", name, *obj_ptrs);

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    va_list args;
    va_copy(args,arguments);

    auto log = get_logger();
    log->trace("file_get: file = {}, get_type = {} req = {} dxpl_id = {}",
            *file_, get_type, fmt::ptr(req), dxpl_id);
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(file_))
        result = VOLBase::file_get(unwrap(file_), get_type, dxpl_id, req, arguments);
    else
    {
        // see hdf5 H5VLnative_file.c, H5VL__native_file_get()
        if (get_type == H5VL_FILE_GET_CONT_INFO)            // file container info
        {
            H5VL_file_cont_info_t *info = va_arg(arguments, H5VL_file_cont_info_t *);

            /* Verify structure version */
            assert(info->version == H5VL_CONTAINER_INFO_VERSION);

            /* Set the container info fields */
            info->feature_flags = 0;
            info->token_size    = 8;
            info->blob_id_size  = 8;
        }
        else if (get_type == H5VL_FILE_GET_FAPL)            // file access property list
        {
            hid_t* plist_id = va_arg(arguments, hid_t*);
            *plist_id = static_cast<File*>(file_->mdata_obj)->fapl.id;
        }
        else if (get_type == H5VL_FILE_GET_FCPL)            // file creation property list
        {
            hid_t* plist_id = va_arg(arguments, hid_t*);
            *plist_id = static_cast<File*>(file_->mdata_obj)->fcpl.id;
        }
        // TODO
        else if (get_type == H5VL_FILE_GET_FILENO)          // file number
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_FILENO not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_INTENT)          // file intent
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_INTENT not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_NAME)            // file name
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_NAME not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_OBJ_COUNT)       // file object count
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_OBJ_COUNT not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_OBJ_IDS)         // file object ids
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_OBJ_IDS not implemented in memory yet"));
        else
            throw MetadataError(fmt::format("requested file_get(), unrecognized get_type = {}", get_type));
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;
    auto log = get_logger();
    log->trace("MetadataVOL::file_close: {}", *file_);

    if (file_->tmp)
    {
        log->trace("temporary reference, skipping close");
        return 0;
    }

    herr_t res = 0;

    if (unwrap(file_))
        res = VOLBase::file_close(unwrap(file_), dxpl_id, req);

    // TODO: add DummyFile condition
    if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        // we created this file
        if (LowFive::get_log_level() <= spdlog::level::info)
            f->print();

        if (!keep)
        {
            files.erase(f->name);
            delete f;       // erases all the children too
        } else
            log->trace("Keeping file metadata in memory");
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
    ObjectPointers* file_ = (ObjectPointers*) file;
    auto log = get_logger();
    log->trace("file_specific: {}", *file_);

    // debug
    if (specific_type == H5VL_FILE_FLUSH)
        log->trace("file_specific(): specific_type = H5VL_FILE_FLUSH");

    if (unwrap(file_))
        return VOLBase::file_specific(unwrap(file_), specific_type, dxpl_id, req, arguments);

    else if (file_->mdata_obj)
    {
        if (specific_type == H5VL_FILE_FLUSH)
        {
            log->trace("file_specific(): specific_type = H5VL_FILE_FLUSH: no-op for metadata");
            return 0;
        }
        else
            throw MetadataError(fmt::format("file_specific(): specific_type {} not implemented for in-memory regime", specific_type));
    }

    else
        throw MetadataError(fmt::format("file_specific(): neither passthru nor metadata are active"));
}
