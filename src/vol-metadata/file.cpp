#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include <cassert>
#include "../log-private.hpp"

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    if ((flags & H5F_ACC_EXCL) &&
        match_any(name, "", memory, true) &&
        files.find(name) != files.end())
        return nullptr;
    void* underlying = nullptr;
    if (match_any(name, "", passthru, true))
    {
        underlying = VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    drop(std::string(name));
    auto* metadata = new File(name, fcpl_id, fapl_id);
    files.emplace(name, metadata);
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->file_wrapper = true;
    result->primary_file_wrapper = true;
    result->access_flags = H5F_ACC_RDWR;
    result->file_handle = next_file_handle++;
    return result;
}


void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    void* underlying = nullptr;
    if (match_any(name, "", passthru, true))
    {
        underlying = VOLBase::file_open(name, flags, fapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    auto it = files.find(name);
    if (it == files.end())
    {
        if (!underlying) return nullptr;
        auto* metadata = new File(name, H5P_FILE_CREATE_DEFAULT, fapl_id);
        it = files.emplace(name, metadata).first;
    }
    if (auto* metadata = dynamic_cast<File*>(it->second))
        metadata->fapl = Hid(H5Pcopy(fapl_id), true);
    auto* result = wrap(underlying);
    result->mdata_obj = it->second;
    result->file_wrapper = true;
    result->primary_file_wrapper = true;
    result->access_flags = flags;
    result->file_handle = next_file_handle++;
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

    auto* object = static_cast<Object*>(file_->mdata_obj);
    if (file_->reference_file_wrapper && object)
        if (auto* root = dynamic_cast<File*>(object->find_root()))
            root->reference_file_id = H5I_INVALID_HID;

    herr_t res = 0;
    if (unwrap(file_))
        res = VOLBase::file_close(unwrap(file_), dxpl_id, req);
    if (res < 0) return res;
    if (after_file_close && object)
        after_file_close(object->find_root()->name);
    return res;
}

herr_t
LowFive::MetadataVOL::
file_specific(void *file, H5VL_file_specific_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;
    auto log = get_logger();

    // special case, when checking for accessibility
    if (!file_)
    {
        log->trace("file_specific: file == 0");

        if (args->op_type == H5VL_FILE_IS_ACCESSIBLE)
        {
            log->trace("file_specific: looking up {}", args->args.is_accessible.filename);

            std::string name = args->args.is_accessible.filename;

            hbool_t result = false;
            if (match_any(name, "", passthru, true))
            {
                *args->args.is_accessible.accessible = false;
                auto status = VOLBase::file_specific(file, args, dxpl_id, req);
                if (status < 0) return status;
                result = *args->args.is_accessible.accessible;
                log->trace("file_specific: passthru result = {}", result);
            }
            else if (match_any(name, "", memory, true))
            {
                result = files.find(name) != files.end();
                log->trace("file_specific: memory result = {}", result);
            }

            *args->args.is_accessible.accessible = result;

            return 0;
        } else
            throw MetadataError(fmt::format("file_specific(): file == 0, but specific_type = {} != {} (H5VL_FILE_IS_ACCESSIBLE)", args->op_type, H5VL_FILE_IS_ACCESSIBLE));
    }

    log->trace("file_specific: {}", *file_);

    // debug
    if (args->op_type == H5VL_FILE_FLUSH)
        log->trace("file_specific(): specific_type = H5VL_FILE_FLUSH");

    if (unwrap(file_))
    {
        // warn if metadata are available but won't be used
        if (file_->mdata_obj)
            log->warn("Warning: file_specific: obj = {} metadata are available but won't be used", *file_);

        herr_t result = VOLBase::file_specific(unwrap(file_), args, dxpl_id, req);
        if (result >= 0 && args->op_type == H5VL_FILE_REOPEN)
        {
            auto* reopened = wrap(*args->args.reopen.file);
            reopened->mdata_obj = file_->mdata_obj;
            reopened->file_wrapper = true;
            reopened->primary_file_wrapper = true;
            reopened->access_flags = file_->access_flags;
            reopened->file_handle = next_file_handle++;
            *args->args.reopen.file = reopened;
        }
        return result;
    }
    else if (file_->mdata_obj)
    {
        if (args->op_type == H5VL_FILE_FLUSH)
        {
            log->trace("file_specific(): specific_type = H5VL_FILE_FLUSH: no-op for metadata");
            return 0;
        }
        else if (args->op_type == H5VL_FILE_REOPEN)
        {
            auto* reopened = wrap(nullptr);
            reopened->mdata_obj = file_->mdata_obj;
            reopened->file_wrapper = true;
            reopened->primary_file_wrapper = true;
            reopened->access_flags = file_->access_flags;
            reopened->file_handle = next_file_handle++;
            *args->args.reopen.file = reopened;
            return 0;
        }
        else
            throw MetadataError(fmt::format("file_specific(): specific_type {} not implemented for in-memory regime", args->op_type));
    }

    else
        throw MetadataError(fmt::format("file_specific(): neither passthru nor metadata are active"));
}


herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    auto get_type = args->op_type;

    auto log = get_logger();
    log->trace("file_get: file = {}, get_type = {} req = {} dxpl_id = {}",
            *file_, args->op_type, fmt::ptr(req), dxpl_id);
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (unwrap(file_))
    {
        // warn if metadata are available but won't be used
        if (file_->mdata_obj)
            log->warn("Warning: file_get: obj = {} metadata are available but won't be used", *file_);

        // NB: The case of H5VL_FILE_GET_OBJ_IDS requires that we augment the
        //     output with mdata_obj pointers.  The native implementation seems to
        //     do this automagically.
        log->trace("file_get(): fall through to VOLBase");
        result = VOLBase::file_get(unwrap(file_), args, dxpl_id, req);
    }
    else
    {
        // see hdf5 H5VLnative_file.c, H5VL__native_file_get()
        if (get_type == H5VL_FILE_GET_CONT_INFO)            // file container info
        {
            H5VL_file_cont_info_t *info = args->args.get_cont_info.info;

            /* Verify structure version */
            assert(info->version == H5VL_CONTAINER_INFO_VERSION);

            /* Set the container info fields */
            info->feature_flags = 0;
            info->token_size    = 8;
            info->blob_id_size  = 8;
        }
        else if (get_type == H5VL_FILE_GET_FAPL)            // file access property list
        {
            log->trace("file_get(): H5VL_FILE_GET_FAPL, writing {} into args", static_cast<File*>(file_->mdata_obj)->fapl.id);
            args->args.get_fapl.fapl_id = H5Pcopy(static_cast<File*>(file_->mdata_obj)->fapl.id);
        }
        else if (get_type == H5VL_FILE_GET_FCPL)            // file creation property list
        {
            log->trace("file_get(): H5VL_FILE_GET_FCPL, writing {} into args", static_cast<File*>(file_->mdata_obj)->fcpl.id);
            args->args.get_fcpl.fcpl_id = H5Pcopy(static_cast<File*>(file_->mdata_obj)->fcpl.id);
        }
            // TODO
        else if (get_type == H5VL_FILE_GET_FILENO)          // file number
//            args->args.get_fileno.fileno = ???
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_FILENO not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_INTENT)          // file intent
        {
            *args->args.get_intent.flags = file_->access_flags;
        }
        else if (get_type == H5VL_FILE_GET_NAME)            // file name
        {
            size_t     size = args->args.get_name.buf_size;
            char *     name = args->args.get_name.buf;
            size_t*    len  = args->args.get_name.file_name_len;

            auto* name_c = static_cast<File*>(file_->mdata_obj)->name.c_str();
            *len = std::strlen(name_c);

            if (name && size) {
                std::strncpy(name, name_c, std::min(*len + 1, size));
                if (*len >= size)
                    name[size - 1] = '\0';
            }
        }
        else if (get_type == H5VL_FILE_GET_OBJ_COUNT ||
                 get_type == H5VL_FILE_GET_OBJ_IDS)
        {
            const bool listing = get_type == H5VL_FILE_GET_OBJ_IDS;
            const unsigned types = listing ? args->args.get_obj_ids.types :
                                             args->args.get_obj_count.types;
            struct Enumeration
            {
                MetadataVOL* vol;
                Object* root;
                uint64_t local_handle;
                hid_t* ids;
                size_t capacity;
                size_t count = 0;
            } state {this, static_cast<Object*>(file_->mdata_obj)->find_root(),
                     (types & H5F_OBJ_LOCAL) ? file_->file_handle : 0,
                     listing ? args->args.get_obj_ids.oid_list : nullptr,
                     listing ? args->args.get_obj_ids.max_objs : 0};
            if (listing && state.capacity && !state.ids) return -1;
            auto visit = [](hid_t id, void* context) -> herr_t
            {
                auto& state = *static_cast<Enumeration*>(context);
                if (H5Iget_type(id) == H5I_DATATYPE && H5Tcommitted(id) <= 0)
                    return 0;
                void* raw = H5VLobject(id);
                if (!raw || state.vol->our_objects.count(raw) == 0) return 0;
                auto* wrapped = static_cast<ObjectPointers*>(raw);
                auto* metadata = static_cast<Object*>(wrapped->mdata_obj);
                if (!metadata || metadata->find_root() != state.root) return 0;
                if (state.local_handle && wrapped->file_handle != state.local_handle)
                    return 0;
                if (state.ids && state.count < state.capacity)
                    state.ids[state.count] = id;
                ++state.count;
                return 0;
            };
            const std::pair<unsigned, H5I_type_t> classes[] = {
                {H5F_OBJ_FILE, H5I_FILE}, {H5F_OBJ_DATASET, H5I_DATASET},
                {H5F_OBJ_GROUP, H5I_GROUP}, {H5F_OBJ_DATATYPE, H5I_DATATYPE},
                {H5F_OBJ_ATTR, H5I_ATTR}
            };
            for (const auto& entry : classes)
                if ((types & entry.first) &&
                    H5Iiterate(entry.second, visit, &state) < 0) return -1;
            if (listing)
                *args->args.get_obj_ids.count = std::min(state.count, state.capacity);
            else
                *args->args.get_obj_count.count = state.count;
        }
        else
            throw MetadataError(fmt::format("requested file_get(), unrecognized get_type = {}", get_type));
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    auto log = get_logger();
    log->trace("file_optional: file = {}, opt_type = {}", *file_, args->op_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    herr_t res = -1;
    if (unwrap(file_))
    {
        // warn if metadata are available but won't be used
        if (file_->mdata_obj)
            log->warn("Warning: file_optional: obj = {} metadata are available but won't be used", *file_);

        res = VOLBase::file_optional(unwrap(file_), args, dxpl_id, req);
    }

    return res;
}

bool LowFive::MetadataVOL::
has_real_file(const char* name) const
{
    auto it = files.find(std::string(name));
    if (it == files.end())
        return false;

    return dynamic_cast<File*>(it->second) != nullptr;
}
