#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <assert.h>

herr_t
LowFive::MetadataVOL::
link_create(H5VL_link_create_type_t create_type, void *obj,
        const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id,
        hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("link_create: obj = {}, create_type = {}", *obj_, create_type);

    herr_t res = 0;

    // see hdf5 src/H5VLnative_link.c, H5VL__native_link_create()

    // both metadata and native objects need to be linked, so two separate if statements
    // but first we need to extract the shared arguments
    //
    void* cur_obj;
    H5VL_loc_params_t* cur_params;
    char* target_name;
    if(H5VL_LINK_CREATE_HARD == create_type)
    {
        /* Retrieve the object & loc params for the link target */
        cur_obj = va_arg(arguments, void *);
        cur_params = va_arg(arguments, H5VL_loc_params_t*);
    } else if (H5VL_LINK_CREATE_SOFT == create_type)
    {
        target_name = va_arg(arguments, char*);
    }

    if (obj_->mdata_obj)
    {
        if(H5VL_LINK_CREATE_HARD == create_type)
        {
            // the only combination we currently support (needed for h5py)
            assert(loc_params->type == H5VL_OBJECT_BY_NAME);
            std::string name = loc_params->loc_data.loc_by_name.name;
            assert(cur_obj);
            ObjectPointers* cur_obj_ = (ObjectPointers*) cur_obj;
            log->trace("  cur_obj = {}", *cur_obj_);

            if (cur_params->type == H5VL_OBJECT_BY_SELF)
            {
                // TODO: probably should handle the path, in case it can be given
                assert(name.find("/") == std::string::npos);      // expecting just a name, not a path
                static_cast<Object*>(cur_obj_->mdata_obj)->name = name;
            } else if (cur_params->type == H5VL_OBJECT_BY_NAME)
            {
                // (obj, loc_params) -> (cur_obj, cur_params)
                auto obj_path = static_cast<Object*>(obj_->mdata_obj)->locate(*loc_params);
                assert(obj_path.is_name());

                auto cur_obj_path = static_cast<Object*>(cur_obj_->mdata_obj)->locate(*cur_params);
                Object* cur_obj = cur_obj_path.exact();     // assert that we found it
                obj_path.obj->add_child(new HardLink(obj_path.path, cur_obj));
            } else if (cur_params->type == H5VL_OBJECT_BY_TOKEN)
            {
                // TODO
                throw MetadataError(fmt::format("link_create(): don't yet support cur_params->type = H5VL_OBJECT_BY_TOKEN"));
            } else if (cur_params->type == H5VL_OBJECT_BY_IDX)
            {
                // TODO
                throw MetadataError(fmt::format("link_create(): don't yet support cur_params->type = H5VL_OBJECT_BY_IDX"));
            } else
                throw MetadataError(fmt::format("link_create(): don't recognize cur_params->type = {}", cur_params->type));
        } else if(H5VL_LINK_CREATE_SOFT == create_type)
        {
                // (obj, loc_params) -> target_name
                auto obj_path = static_cast<Object*>(obj_->mdata_obj)->locate(*loc_params);
                assert(obj_path.is_name());
                obj_path.obj->add_child(new SoftLink(obj_path.path, target_name));
        } else
        {
            throw MetadataError(fmt::format("link_create(): unsupported create_type = {}", create_type));
        }
    }

    if (unwrap(obj_))
    {
        if(H5VL_LINK_CREATE_HARD == create_type)
        {
            if (cur_obj)
                cur_obj = unwrap(cur_obj);

            res = link_create_trampoline(create_type, unwrap(obj_), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req,
                        cur_obj, cur_params);
        } else if(H5VL_LINK_CREATE_SOFT == create_type)
        {
            res = link_create_trampoline(create_type, unwrap(obj_), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, target_name);
        } else
        {
            res = link_create_trampoline(create_type, unwrap(obj_), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
        }
    }

    return res;
}

herr_t
LowFive::MetadataVOL::
link_create_trampoline(H5VL_link_create_type_t create_type, void *obj,
        const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id,
        hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = VOLBase::link_create(create_type, obj, loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
}

herr_t
LowFive::MetadataVOL::
link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
        void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
        hid_t lapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* src_obj_ = (ObjectPointers*)src_obj;
    ObjectPointers* dst_obj_ = (ObjectPointers*)dst_obj;

    auto log = get_logger();
    log->trace("link_copy: src_obj = {}, dst_obj = {}", *src_obj_, *dst_obj_);

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_copy(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else if (src_obj_->mdata_obj && dst_obj_->mdata_obj)
        log->warn("Warning: link_copy not implemented in metadata yet");
    else
        throw MetadataError(fmt::format("link_copy(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
        void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
        hid_t lapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* src_obj_ = (ObjectPointers*)src_obj;
    ObjectPointers* dst_obj_ = (ObjectPointers*)dst_obj;

    auto log = get_logger();
    log->trace("link_move: src_obj = {}, dst_obj = {}", *src_obj_, *dst_obj_);

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_move(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else if (src_obj_->mdata_obj && dst_obj_->mdata_obj)
        log->warn("Warning: link_move not implemented in metadata yet");
    else
        throw MetadataError(fmt::format("link_move(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_get(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
        H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("link_get: obj = {}, get_type = {}", *obj_, get_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_get(unwrap(obj_), loc_params, under_vol_id, get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
        log->warn("Warning: link_get not implemented in metadata yet");
    else
        throw MetadataError(fmt::format("link_get(): either passthru or metadata must be specified"));

    return res;
}

// helper function for link_specific()
herr_t
LowFive::MetadataVOL::
link_iter(void *obj, va_list arguments)
{
    ObjectPointers* obj_        = (ObjectPointers*) obj;
    Object*         mdata_obj   = static_cast<Object*>(obj_->mdata_obj);

    // copied from HDF5 H5VLnative_link.c, H5VL__native_link_specific()
    hbool_t         recursive = (hbool_t)va_arg(arguments, unsigned);
    H5_index_t      idx_type  = (H5_index_t)va_arg(arguments, int);
    H5_iter_order_t order     = (H5_iter_order_t)va_arg(arguments, int);
    hsize_t *       idx_p     = va_arg(arguments, hsize_t *);
    H5L_iterate2_t  op        = va_arg(arguments, H5L_iterate2_t);
    void *          op_data   = va_arg(arguments, void *);

    auto log = get_logger();

    // get object type in HDF format and use that to get an HDF hid_t to the object
    std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
    if (static_cast<int>(mdata_obj->type) >= h5_types.size())     // sanity check
        throw MetadataError(fmt::format("link_iter(): mdata_obj->type {} > H5I_DATATYPE, the last element of h5_types", mdata_obj->type));
    int obj_type = h5_types[static_cast<int>(mdata_obj->type)];
    ObjectPointers* obj_tmp = wrap(nullptr);
    *obj_tmp = *obj_;
    obj_tmp->tmp = true;
    log->trace("link_iter: wrapping object type mdata_obj->type {} h5 obj_type {}", *obj_tmp, mdata_obj->type, obj_type);

    hid_t obj_loc_id = H5VLwrap_register(obj_tmp, static_cast<H5I_type_t>(obj_type));

    // info for link, defined in HDF5 H5Apublic.h  TODO: assigned with some defaults, not sure if they're corrent
    H5L_info_t linfo;
    linfo.corder_valid  = true;                     // whether creation order is valid
    linfo.corder        = 0;                        // creation order (TODO: no idea what this is)
    linfo.cset          = H5T_CSET_ASCII;           // character set of link names
    linfo.u.val_size    = 0;                        // union of either token or val_size

    log->trace("link_iter: iterating over direct children of object name {} in order children were created (ignoring itration order and index)",
            mdata_obj->name);
    if (idx_p)
        log->trace("link_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is {}", order, *idx_p);
    else
        log->trace("link_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is unassigned", order);

    if (recursive)
        throw MetadataError(fmt::format("link_iter: recursive iteration not implemented yet"));

    herr_t retval = 0;

    // TODO: currently ignores the iteration order and current index
    // just blindly goes through all the links in the order they were created
    // also ignoring recursive flag (controls whether to iterate / visit, see H5VL__native_link_specific)
    for (auto& c : mdata_obj->children)
    {
        // top-level attributes cannot be objects, skip over them
        if (c->type == ObjectType::Attribute &&
                (mdata_obj->type != ObjectType::Group && mdata_obj->type != ObjectType::Dataset && mdata_obj->type != ObjectType::NamedDtype))
        {
            log->trace("link_iter: skipping mdata object named {} type {}", mdata_obj->name, mdata_obj->type);
            continue;
        }

        // TODO: for now assume all the objects in our metadata are equivalent to a hard link
        linfo.type = H5L_TYPE_HARD;

        log->trace("link_iter: visiting object name {}", c->name);

        retval = (op)(obj_loc_id, c->name.c_str(), &linfo, op_data);
        if (retval > 0)
        {
            log->trace("link_iter: terminating iteration because operator returned > 0 value, indicating user-defined success and early termination");
            break;
        }
        else if (retval < 0)
        {
            log->trace("link_iter: terminating iteration because operator returned < 0 value, indicating user-defined failure and early termination");
            break;
        }
    }   // for all children

    return retval;
}

herr_t
LowFive::MetadataVOL::
link_specific(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
        H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    // see hdf5 src/H5VLnative_link.c, H5VL__native_link_specific()
    // enum of specific types is in H5VLconnector.h

    ObjectPointers* obj_ = (ObjectPointers*)obj;
    auto* mdata_obj = static_cast<Object*>(obj_->mdata_obj);

    auto log = get_logger();
    log->trace("link_specific: obj = {}, specific_type = {}", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_specific(unwrap(obj_), loc_params, under_vol_id, specific_type, dxpl_id, req, arguments);
    else if (mdata_obj)
    {
        if (specific_type == H5VL_LINK_DELETE)               // H5Ldelete(_by_name/idx)
        {
            // TODO
            throw MetadataError(fmt::format("H5VL_LINK_DELETE not yet implemented in LowFive::MetadataVOL::link_specific()"));
        }
        else if (specific_type == H5VL_LINK_EXISTS)         // H5Lexists(_by_name)
        {
            log->trace("link_specific: specific_type H5VL_LINK_EXISTS");
            htri_t *  ret = va_arg(arguments, htri_t *);
            *ret = -1;

            auto op = static_cast<Object*>(mdata_obj)->locate(*loc_params);
            *ret = op.path.empty();
            res = *ret;
        }
        else if (specific_type == H5VL_LINK_ITER)           // H5Liter/H5Lvisit(_by_name/_by_self)
        {
            log->trace("link_specific: specific_type H5VL_LINK_ITER");

            // sanity check that the provided object matches the location parameters
            // ie,  we're not supposed to operate on one of the children instead of the parent (which we don't support)
            if (mdata_obj != mdata_obj->locate(*loc_params).exact())
                throw MetadataError(fmt::format("link_specific: specific_type H5VL_LINK_ITER, object does not match location parameters"));

            res = link_iter(obj, arguments);
        }
        else
            throw MetadataError(fmt::format("link_specific unrecognized specific_type = {}", specific_type));
    }
    else
        throw MetadataError(fmt::format("link_specific(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_optional(void *obj, hid_t under_vol_id, H5VL_link_optional_t opt_type,
        hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("link_optional: obj = {}, optional_type = {}", *obj_, opt_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_optional(unwrap(obj_), under_vol_id, opt_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
        log->warn("Warning: link_optional not implemented in metadata yet");
    else
        throw MetadataError(fmt::format("link_optional(): either passthru or metadata must be specified"));

    return res;
}


