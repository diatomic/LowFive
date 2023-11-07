#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <assert.h>

herr_t
LowFive::MetadataVOL::link_create(H5VL_link_create_args_t* args, void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*)obj;

    auto log = get_logger();
    log->trace("link_create: obj = {}, create_type = {}", *obj_, args->op_type);

    herr_t res = 0;

    // see hdf5 src/H5VLnative_link.c, H5VL__native_link_create()

    if (obj_)
    {
        if(H5VL_LINK_CREATE_HARD == args->op_type)
        {
            // the only combination we currently support (needed for h5py)
            assert(loc_params->type == H5VL_OBJECT_BY_NAME);
            std::string name = loc_params->loc_data.loc_by_name.name;
            assert(args->args.hard.curr_obj);
            Object* cur_obj_ = (Object*) args->args.hard.curr_obj;
            log->trace("  cur_obj = {}", *cur_obj_);

            if (args->args.hard.curr_loc_params.type == H5VL_OBJECT_BY_SELF)
            {
                // TODO: probably should handle the path, in case it can be given
                assert(name.find("/") == std::string::npos);      // expecting just a name, not a path
                cur_obj_->name = name;
            } else if (args->args.hard.curr_loc_params.type == H5VL_OBJECT_BY_NAME)
            {
                // (obj, loc_params) -> (cur_obj, cur_params)
                auto obj_path = obj_->locate(*loc_params);
                assert(obj_path.is_name());

                auto cur_obj_path = cur_obj_->locate(args->args.hard.curr_loc_params);
                Object* cur_obj = cur_obj_path.exact();     // assert that we found it
                obj_path.obj->add_child(new HardLink(obj_path.path, cur_obj));
            } else if (args->args.hard.curr_loc_params.type == H5VL_OBJECT_BY_TOKEN)
            {
                // TODO
                throw MetadataError(fmt::format("link_create(): don't yet support cur_params->type = H5VL_OBJECT_BY_TOKEN"));
            } else if (args->args.hard.curr_loc_params.type == H5VL_OBJECT_BY_IDX)
            {
                // TODO
                throw MetadataError(fmt::format("link_create(): don't yet support cur_params->type = H5VL_OBJECT_BY_IDX"));
            } else
                throw MetadataError(fmt::format("link_create(): don't recognize cur_params->type = {}", args->args.hard.curr_loc_params.type));
        } else if(H5VL_LINK_CREATE_SOFT == args->op_type)
        {
                // (obj, loc_params) -> target_name
                auto obj_path = obj_->locate(*loc_params);
                assert(obj_path.is_name());
                obj_path.obj->add_child(new SoftLink(obj_path.path, args->args.soft.target));
        } else
        {
            throw MetadataError(fmt::format("link_create(): unsupported create_type = {}", args->op_type));
        }
    }

    if (unwrap(obj_))
    {
        if(H5VL_LINK_CREATE_HARD == args->op_type)
        {
            // AN: modifying the args given to us to call trampoline, hopefully copying is not necessary
            if (args->args.hard.curr_obj)
                args->args.hard.curr_obj = unwrap((Object*)args->args.hard.curr_obj);

            res = link_create_trampoline(args, unwrap(obj_), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        } else
        {
            // in 1.14, no need to check anything else
            res = link_create_trampoline(args, unwrap(obj_), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        }
    }

    return res;
}

herr_t
LowFive::MetadataVOL::link_create_trampoline(H5VL_link_create_args_t* args, void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req)
{
    return VOLBase::link_create(args, obj, loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
}

herr_t
LowFive::MetadataVOL::
link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
        void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
        hid_t lapl_id, hid_t dxpl_id, void **req)
{
    Object* src_obj_ = (Object*)src_obj;
    Object* dst_obj_ = (Object*)dst_obj;

    if (!src_obj_ || !dst_obj_)
        throw MetadataError(fmt::format("link_copy(): got null pointers"));

    auto log = get_logger();
    log->trace("link_copy: src_obj = {}, dst_obj = {}", *src_obj_, *dst_obj_);

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_copy(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else
        throw MetadataError(fmt::format("link_copy(): not implemented in metadata yet"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
        void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
        hid_t lapl_id, hid_t dxpl_id, void **req)
{
    Object* src_obj_ = (Object*)src_obj;
    Object* dst_obj_ = (Object*)dst_obj;

    auto log = get_logger();
    log->trace("link_move: src_obj = {}, dst_obj = {}", *src_obj_, *dst_obj_);

    if ((src_obj == nullptr) || (dst_obj == nullptr))
        throw MetadataError(fmt::format("link_copy(): got null pointers"));

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_move(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else
    {
        log->warn("Warning: link_move not implemented in metadata yet");
        throw MetadataError(fmt::format("link_move(): not implemented in metadata yet"));
    }

    return res;
}

herr_t
LowFive::MetadataVOL::link_get(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_link_get_args_t* args, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*)obj;

    auto log = get_logger();
    log->trace("link_get: obj = {}, get_type = {}", *obj_, args->op_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_get(unwrap(obj_), loc_params, under_vol_id, args, dxpl_id, req);
    else
    {
        if (args->op_type == H5VL_LINK_GET_NAME)
        {
            auto* o = obj_->locate(*loc_params).exact();
            auto name_size = o->name.size();
            if (args->args.get_name.name && args->args.get_name.name_size >= name_size)
                memcpy(args->args.get_name.name, o->name.c_str(), name_size + 1);
            *args->args.get_name.name_len = name_size + 1;                     // should this be +1 for c-string?
        } else
        {
            log->warn("Warning: link_get not implemented in metadata yet");
            throw MetadataError(fmt::format("link_get not implemented in metadata yet"));
        }
    }
//    else
//        throw MetadataError(fmt::format("link_get(): either passthru or metadata must be specified"));

    return res;
}

// helper function for link_specific()
herr_t
LowFive::MetadataVOL::
link_iter(void *obj, H5VL_link_specific_args_t* args)
{
    if (args->op_type != H5VL_LINK_ITER)
        throw MetadataError("called link_iter, but op_type is not H5VL_LINK_ITER");

    Object* obj_ = (Object*) obj;

    auto log = get_logger();

    // get object type in HDF format and use that to get an HDF hid_t to the object
    std::vector<int> h5_types = {H5I_FILE, H5I_GROUP, H5I_DATASET, H5I_ATTR, H5I_DATATYPE};     // map of our object type to hdf5 object types
    if (static_cast<int>(obj_->type) >= h5_types.size())     // sanity check
        throw MetadataError(fmt::format("link_iter(): mdata_obj->type {} > H5I_DATATYPE, the last element of h5_types", obj_->type));
    int obj_type = h5_types[static_cast<int>(obj_->type)];

    hid_t obj_loc_id = H5VLwrap_register(obj_, static_cast<H5I_type_t>(obj_type));

    // info for link, defined in HDF5 H5Apublic.h  TODO: assigned with some defaults, not sure if they're corrent
    H5L_info_t linfo;
    linfo.corder_valid  = true;                     // whether creation order is valid
    linfo.corder        = 0;                        // creation order (TODO: no idea what this is)
    linfo.cset          = H5T_CSET_ASCII;           // character set of link names
    linfo.u.val_size    = 0;                        // union of either token or val_size

    log->trace("link_iter: iterating over direct children of object name {} in order children were created (ignoring itration order and index)",
            obj_->name);
    if (args->args.iterate.idx_p)
        log->trace("link_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is {}", args->args.iterate.order, *args->args.iterate.idx_p);
    else
        log->trace("link_iter: the provided order (H5_iter_order_t in H5public.h) is {} and the current index is unassigned", args->args.iterate.order);

    if (args->args.iterate.recursive)
        throw MetadataError(fmt::format("link_iter: recursive iteration not implemented yet"));

    herr_t retval = 0;

    // TODO: currently ignores the iteration order and current index
    // just blindly goes through all the links in the order they were created
    // also ignoring recursive flag (controls whether to iterate / visit, see H5VL__native_link_specific)
    for (auto c : obj_->children)
    {
        // top-level attributes cannot be objects, skip over them
        if (c->type == ObjectType::Attribute &&
                (obj_->type != ObjectType::Group && obj_->type != ObjectType::Dataset && obj_->type != ObjectType::NamedDtype))
        {
            log->trace("link_iter: skipping mdata object named {} type {}", obj_->name, obj_->type);
            continue;
        }

        // TODO: for now assume all the objects in our metadata are equivalent to a hard link
        linfo.type = H5L_TYPE_HARD;

        log->trace("link_iter: visiting object name {}", c->name);

        retval = (args->args.iterate.op)(obj_loc_id, c->name.c_str(), &linfo, args->args.iterate.op_data);
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
LowFive::MetadataVOL::link_specific(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_link_specific_args_t* args, hid_t dxpl_id, void** req)
{
    // see hdf5 src/H5VLnative_link.c, H5VL__native_link_specific()
    // enum of specific types is in H5VLconnector.h

    Object* obj_ = (Object*)obj;

    auto log = get_logger();
    log->trace("link_specific: obj = {}, specific_type = {}", *obj_, args->op_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_specific(unwrap(obj_), loc_params, under_vol_id, args, dxpl_id, req);
    else
    {
        if (args->op_type == H5VL_LINK_DELETE)               // H5Ldelete(_by_name/idx)
        {
            // TODO
            throw MetadataError(fmt::format("H5VL_LINK_DELETE not yet implemented in LowFive::MetadataVOL::link_specific()"));
        }
        else if (args->op_type == H5VL_LINK_EXISTS)         // H5Lexists(_by_name)
        {
            log->trace("link_specific: specific_type H5VL_LINK_EXISTS");

            auto op = obj_->locate(*loc_params);
            *(args->args.exists.exists) = op.path.empty();
            res = op.path.empty();
        }
        else if (args->op_type == H5VL_LINK_ITER)           // H5Liter/H5Lvisit(_by_name/_by_self)
        {
            log->trace("link_specific: specific_type H5VL_LINK_ITER");

            // sanity check that the provided object matches the location parameters
            // ie,  we're not supposed to operate on one of the children instead of the parent (which we don't support)
            if (obj_ != obj_->locate(*loc_params).exact())
                throw MetadataError(fmt::format("link_specific: specific_type H5VL_LINK_ITER, object does not match location parameters"));

            res = link_iter(obj, args);
        }
        else
            throw MetadataError(fmt::format("link_specific unrecognized specific_type = {}", args->op_type));
    }

    return res;
}

herr_t
LowFive::MetadataVOL::link_optional(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    Object* obj_ = (Object*)obj;

    auto log = get_logger();
    log->trace("link_optional: obj = {}, optional_type = {}", *obj_, args->op_type);

    if (!obj)
        throw MetadataError(fmt::format("link_optional(): either passthru or metadata must be specified"));

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_optional(unwrap(obj_), loc_params, under_vol_id, args, dxpl_id, req);
    else
    {
        log->warn("Warning: link_optional not implemented in metadata yet");
        throw MetadataError(fmt::format("link_optional(): not implemented in metadata yet"));
    }

    return res;
}
