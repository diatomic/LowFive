#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include "../log-private.hpp"
#include <memory>

namespace
{
struct PropertyList
{
    hid_t id;
    ~PropertyList() { if (id >= 0) H5Pclose(id); }
};
}


/*-------------------------------------------------------------------------
 * Function:    file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t* configuration = nullptr;
    if (H5Pget_vol_info(fapl_id, reinterpret_cast<void**>(&configuration)) < 0)
        return nullptr;
    std::unique_ptr<info_t, decltype(&_info_free)> owned(configuration, _info_free);
    PropertyList underlying{H5Pcopy(fapl_id)};
    if (underlying.id < 0 || H5Pset_vol(underlying.id, configuration->under_vol_id, configuration->under_vol_info) < 0)
        return nullptr;
    void* object = configuration->vol->file_create(name, flags, fcpl_id, underlying.id, dxpl_id, req);
    if (!object)
        return nullptr;
    auto* file = new pass_through_t(object, configuration->under_vol_id, configuration->vol);
    if (req && *req)
        *req = file->create(*req);
    return file;
} /* end file_create() */

void*
LowFive::VOLBase::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    return H5VLfile_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t* configuration = nullptr;
    if (H5Pget_vol_info(fapl_id, reinterpret_cast<void**>(&configuration)) < 0)
        return nullptr;
    std::unique_ptr<info_t, decltype(&_info_free)> owned(configuration, _info_free);
    PropertyList underlying{H5Pcopy(fapl_id)};
    if (underlying.id < 0 || H5Pset_vol(underlying.id, configuration->under_vol_id, configuration->under_vol_info) < 0)
        return nullptr;
    void* object = configuration->vol->file_open(name, flags, underlying.id, dxpl_id, req);
    if (!object)
        return nullptr;
    auto* file = new pass_through_t(object, configuration->under_vol_id, configuration->vol);
    if (req && *req)
        *req = file->create(*req);
    return file;
} /* end file_open() */

void*
LowFive::VOLBase::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    return H5VLfile_open(name, flags, fapl_id, dxpl_id, req);
}


/*-------------------------------------------------------------------------
 * Function:    _file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_specific(void *file, H5VL_file_specific_args_t* args, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    //hid_t under_vol_id = -1;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Specific");

    auto* plugin = o ? o->vol : info_t::vol;
    if (!plugin)
        return -1;
    ret_value = plugin->file_specific(o ? o->under_object : nullptr, args, dxpl_id, req);
    if (ret_value >= 0 && args->op_type == H5VL_FILE_REOPEN && args->args.reopen.file && *args->args.reopen.file)
        *args->args.reopen.file = o->create(*args->args.reopen.file);

    /* Check for async request */
    if(req && *req)
        *req = new pass_through_t(*req, o ? o->under_vol_id : info->under_vol_id, plugin);

    return ret_value;
} /* end _file_specific() */

herr_t
LowFive::VOLBase::
file_specific(void *file, H5VL_file_specific_args_t* args, hid_t dxpl_id, void **req)
{
    return H5VLfile_specific(file, info->under_vol_id, args, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_close(void *file, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Close");

    ret_value = o->vol->file_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end file_close() */

herr_t
LowFive::VOLBase::
file_close(void *file, hid_t dxpl_id, void **req)
{
    auto log = get_logger();
    log->trace("VOLBase::file_close {}");

    return H5VLfile_close(file, info->under_vol_id, dxpl_id, req);
}

/////////////////////////////////////



/*-------------------------------------------------------------------------
 * Function:    file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_get(void *file, H5VL_file_get_args_t* args, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL FILE Get");

    auto get_type = args->op_type;

    log->trace("file_get: get_type = {}", get_type);

    ret_value = o->vol->file_get(o->under_object, args, dxpl_id, req);

    if (get_type == H5VL_FILE_GET_OBJ_COUNT)
    {
        unsigned types     = args->args.get_obj_count.types;
        size_t*  count     = args->args.get_obj_count.count;
        log->trace("file_get: H5VL_FILE_GET_OBJ_COUNT, types = {}, ret = {}", types, *count);
    }

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_get() */

herr_t
LowFive::VOLBase::
file_get(void *file, H5VL_file_get_args_t* args, hid_t dxpl_id, void **req)
{
    return H5VLfile_get(file, info->under_vol_id, args, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_file_optional(void *file, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL File Optional");

    ret_value = o->vol->file_optional(o->under_object, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_optional() */

herr_t
LowFive::VOLBase::
file_optional(void *file, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    return H5VLfile_optional(file, info->under_vol_id, args, dxpl_id, req);
}

