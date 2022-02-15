#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
link_create(H5VL_link_create_type_t create_type, void *obj,
        const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id,
        hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    fmt::print(stderr, "link_create: obj = {}, create_type = {}\n", *obj_, create_type);

    herr_t res = 0;

    // both metadata and native objects need to be linked, so two separate if statements
    // but first we need to extract the shared arguments
    //
    void* cur_obj;
    H5VL_loc_params_t cur_params;
    if(H5VL_LINK_CREATE_HARD == create_type)
    {
        /* Retrieve the object & loc params for the link target */
        cur_obj = va_arg(arguments, void *);
        cur_params = va_arg(arguments, H5VL_loc_params_t);
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
            fmt::print(stderr, "  cur_obj = {}\n", *cur_obj_);

            if (cur_params.type == H5VL_OBJECT_BY_SELF)
            {
                // TODO: probably should handle the path, in case it can be given
                assert(name.find("/") == std::string::npos);      // expecting just a name, not a path
                static_cast<Object*>(cur_obj_->mdata_obj)->name = name;
            } else if (cur_params.type == H5VL_OBJECT_BY_NAME)
            {
                // (obj, loc_params) -> (cur_obj, cur_params)
                auto obj_path = static_cast<Object*>(obj_->mdata_obj)->locate(*loc_params);
                assert(obj_path.is_name());

                auto cur_obj_path = static_cast<Object*>(cur_obj_->mdata_obj)->locate(cur_params);
                Object* cur_obj = cur_obj_path.exact();     // assert that we found it
                auto target = cur_obj->fullname().second;   // TODO: this is probably the wrong thing to do for hardlink,
                                                            //       the right thing is to link to the object directly,
                                                            //       but Ok for now

                obj_path.obj->add_child(new Link(obj_path.path, true, target));
            } else
                throw MetadataError(fmt::format("link_create(): don't recognize cur_params.type = {}", cur_params.type));
        } else
        {
            throw MetadataError(fmt::format("link_create(): only hard link is implemented in the metadata case"));
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

    fmt::print(stderr, "link_copy: src_obj = {}, dst_obj = {}\n", *src_obj_, *dst_obj_);

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_copy(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else if (src_obj_->mdata_obj && dst_obj_->mdata_obj)
        fmt::print(stderr, "Warning: link_copy not implemented in metadata yet\n");
    else
        throw MetadataError(fmt::format("link_copy(): either passthru or metadata must be specified\n"));

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

    fmt::print(stderr, "link_move: src_obj = {}, dst_obj = {}\n", *src_obj_, *dst_obj_);

    herr_t res = 0;
    if (unwrap(src_obj_) && unwrap(dst_obj_))
        res = VOLBase::link_move(unwrap(src_obj_), loc_params1, unwrap(dst_obj_), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    else if (src_obj_->mdata_obj && dst_obj_->mdata_obj)
        fmt::print(stderr, "Warning: link_move not implemented in metadata yet\n");
    else
        throw MetadataError(fmt::format("link_move(): either passthru or metadata must be specified\n"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_get(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
        H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    fmt::print(stderr, "link_get: obj = {}, get_type = {}\n", *obj_, get_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_get(unwrap(obj_), loc_params, under_vol_id, get_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
        fmt::print(stderr, "Warning: link_get not implemented in metadata yet\n");
    else
        throw MetadataError(fmt::format("link_get(): either passthru or metadata must be specified\n"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_specific(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id,
        H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    fmt::print(stderr, "link_specific: obj = {}, specific_type = {}\n", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_specific(unwrap(obj_), loc_params, under_vol_id, specific_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
        fmt::print(stderr, "Warning: link_specific not implemented in metadata yet\n");
    else
        throw MetadataError(fmt::format("link_specific(): either passthru or metadata must be specified\n"));

    return res;
}

herr_t
LowFive::MetadataVOL::
link_optional(void *obj, hid_t under_vol_id, H5VL_link_optional_t opt_type,
        hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    fmt::print(stderr, "link_optional: obj = {}, optional_type = {}\n", *obj_, opt_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::link_optional(unwrap(obj_), under_vol_id, opt_type, dxpl_id, req, arguments);
    else if (obj_->mdata_obj)
        fmt::print(stderr, "Warning: link_optional not implemented in metadata yet\n");
    else
        throw MetadataError(fmt::format("link_optional(): either passthru or metadata must be specified\n"));

    return res;
}


