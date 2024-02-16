#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
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


void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    // DM: moved to the beginning of DistMetadataVOL::file_open();
    //     if there is a reason to have it here, I'd like to know about it
    //if (before_file_open)
    //{
    //    std::string name_(name);
    //    before_file_open(name_);
    //}

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

//    if (match_any(name, "", passthru, true) && !match_any(name, "", memory, true))
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

    std::string filename;

    // TODO: add DummyFile condition
    if (File* f = dynamic_cast<File*>((Object*) file_->mdata_obj))
    {
        log->trace("mdata_obj is File*");
        filename = f->name;

        // we created this file
        if (LowFive::get_log_level() <= spdlog::level::info)
            f->print();

        if (!keep)
        {
            log->trace("keep not set, deleting file");
            files.erase(f->name);
            delete f;       // erases all the children too
        } else
            log->trace("Keeping file metadata in memory");
    }


    if (after_file_close)
        after_file_close(filename);

    log->trace("after_file_close callback done, filename = {}", filename);

    // deliberately verbose, to emphasize checking of res
    if (res == 0)
        return 0;
    else
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

            hbool_t result = true;
            if (match_any(name, "", passthru, true))
            {
                VOLBase::file_specific(file, args, dxpl_id, req);
                result = result and (*args->args.is_accessible.accessible);
                log->trace("file_specific: passthru result = {}", result);
            }

            if (match_any(name, "", memory, true))
            {
                result = result and (files.find(name) != files.end());
                log->trace("file_specific: memory result = {}", result);
            }

            // DM (2024-02-13): temporary "fix" for moab. Assume that if we are
            // asked, the file should be there. The proper fix requires remote
            // querying in DistMetadataVOL. (FIXME)
            result = true;

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
//        if (args->op_type == H5VL_FILE_IS_EQUAL)
//        {
//            // need to unwrap the pointer to another file in args
//            ObjectPointers* other = (ObjectPointers*)args->args.is_equal.obj2;
//            log->trace("MetadataVOL::file_specific: H5VL_FILE_IS_EQUAL, other = {}", fmt::ptr(other));
//            args->args.is_equal.obj2 = other;
//        }
        return VOLBase::file_specific(unwrap(file_), args, dxpl_id, req);
    }

    else if (file_->mdata_obj)
    {
        if (args->op_type == H5VL_FILE_FLUSH)
        {
            log->trace("file_specific(): specific_type = H5VL_FILE_FLUSH: no-op for metadata");
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
        // NB: The case of H5VL_FILE_GET_OBJ_IDS requires that we augment the
        //     output with mdata_obj pointers.  The native implementation seems to
        //     do this automagically.
        result = VOLBase::file_get(unwrap(file_), args, dxpl_id, req);
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
            args->args.get_fapl.fapl_id = static_cast<File*>(file_->mdata_obj)->fapl.id;
        }
        else if (get_type == H5VL_FILE_GET_FCPL)            // file creation property list
        {
            args->args.get_fcpl.fcpl_id = static_cast<File*>(file_->mdata_obj)->fapl.id;
        }
            // TODO
        else if (get_type == H5VL_FILE_GET_FILENO)          // file number
//            args->args.get_fileno.fileno = ???
            throw MetadataError(fmt::format("file_get(): H5VL_FILE_GET_FILENO not implemented in memory yet"));
        else if (get_type == H5VL_FILE_GET_INTENT)          // file intent
        {
            log->warn("file_get(): H5VL_FILE_GET_INTENT forces H5F_ACC_RDWR as a response");
            *args->args.get_intent.flags = H5F_ACC_RDWR;
        }
        else if (get_type == H5VL_FILE_GET_NAME)            // file name
        {
            size_t     size = args->args.get_name.buf_size;
            char *     name = args->args.get_name.buf;
            size_t*    len  = args->args.get_name.file_name_len;

            auto* name_c = static_cast<File*>(file_->mdata_obj)->name.c_str();
            *len = std::strlen(name_c);

            if (args->args.get_name.buf) {
                std::strncpy(name, name_c, std::min(*len + 1, size));
                if (*len >= size)
                    name[size - 1] = '\0';
            }
        }
        else if (get_type == H5VL_FILE_GET_OBJ_COUNT)       // file object count
        {
            size_t   obj_count = 0; /* Number of opened objects */

            unsigned types     = args->args.get_obj_count.types;
            size_t*  ret       = args->args.get_obj_count.count;
            // TODO
            log->warn("FILE_GET_OBJ_COUNT doesn't currently check whether objects belong to the given file");

            //typedef herr_t (*H5I_iterate_func_t)(hid_t id, void *udata);
            auto count = [](hid_t, void* obj_count_) -> herr_t
            {
              ++(*static_cast<size_t*>(obj_count_));
              return 0;
            };

            if (types & H5F_OBJ_FILE)       H5Iiterate(H5I_FILE,     count, &obj_count);
            if (types & H5F_OBJ_GROUP)      H5Iiterate(H5I_GROUP,    count, &obj_count);
            if (types & H5F_OBJ_DATASET)    H5Iiterate(H5I_DATASET,  count, &obj_count);
            // TODO: datatypes are tricky; need to worry whether they are in the file, or immutable, etc.
            //if (types & H5F_OBJ_DATATYPE)   H5Iiterate(H5I_DATATYPE, count, &obj_count);
            if (types & H5F_OBJ_ATTR)       H5Iiterate(H5I_ATTR,     count, &obj_count);

            *ret = obj_count;
        }
        else if (get_type == H5VL_FILE_GET_OBJ_IDS)         // file object ids
        {
            unsigned types     = args->args.get_obj_ids.types;
            size_t   max_objs  = args->args.get_obj_ids.max_objs;
            hid_t *  oid_list  = args->args.get_obj_ids.oid_list;
            size_t*  ret       = args->args.get_obj_ids.count;
            size_t   obj_count = 0; /* Number of opened objects */

            // TODO
            log->warn("FILE_GET_OBJ_IDS doesn't currently check whether objects belong to the given file");

            using ListInfo = std::tuple<size_t*, hid_t**, size_t*, size_t>;
            ListInfo list_info(&max_objs, &oid_list, &obj_count, 0);

            //typedef herr_t (*H5I_iterate_func_t)(hid_t id, void *udata);
            auto get_objs = [](hid_t obj_id, void* list_info_) -> herr_t
            {
              ListInfo* list_info = static_cast<ListInfo*>(list_info_);

              size_t& max_objs  = *std::get<0>(*list_info);
              hid_t*& oid_list  = *std::get<1>(*list_info);
              size_t& obj_count = *std::get<2>(*list_info);
              size_t& n         =  std::get<3>(*list_info);

              ++obj_count;

              if (n < max_objs)
                  oid_list[n++] = obj_id;

              return 0;        // TODO: could stop early
            };

            if (types & H5F_OBJ_FILE)       H5Iiterate(H5I_FILE,     get_objs, &list_info);
            if (types & H5F_OBJ_GROUP)      H5Iiterate(H5I_GROUP,    get_objs, &list_info);
            if (types & H5F_OBJ_DATASET)    H5Iiterate(H5I_DATASET,  get_objs, &list_info);
            // TODO: datatypes are tricky; need to worry whether they are in the file, or immutable, etc.
            //if (types & H5F_OBJ_DATATYPE)   H5Iiterate(H5I_DATATYPE, get_objs, &list_info);
            if (types & H5F_OBJ_ATTR)       H5Iiterate(H5I_ATTR,     get_objs, &list_info);

            *ret = (ssize_t)obj_count;
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

    herr_t res = 0;
    if (unwrap(file_))
        res = VOLBase::file_optional(unwrap(file_), args, dxpl_id, req);

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
