#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <assert.h>

herr_t
LowFive::MetadataVOL::link_create(H5VL_link_create_args_t* args, void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void** req)
{
    auto* destination = static_cast<ObjectPointers*>(obj);
    auto* source = args->op_type == H5VL_LINK_CREATE_HARD
        ? static_cast<ObjectPointers*>(args->args.hard.curr_obj) : nullptr;
    if (!destination) destination = source;
    if (!source) source = destination;
    if (!destination) return -1;
    if (!destination->writable()) return -1;
    const bool native_destination = unwrap(destination);
    const bool native_source = unwrap(source);
    if (!destination->mdata_obj ||
        (args->op_type == H5VL_LINK_CREATE_HARD && !source->mdata_obj))
    {
        if (!native_destination ||
            (args->op_type == H5VL_LINK_CREATE_HARD && !native_source))
            return -1;
        auto native_args = *args;
        if (native_args.op_type == H5VL_LINK_CREATE_HARD)
            native_args.args.hard.curr_obj = unwrap(args->args.hard.curr_obj);
        return link_create_trampoline(
            &native_args, unwrap(obj), loc_params, under_vol_id, lcpl_id,
            lapl_id, dxpl_id, req);
    }
    auto* parent = static_cast<Object*>(destination->mdata_obj);
    auto path = parent->locate_link(*loc_params);
    if (!path.obj || path.path.empty() || !path.is_name()) return -1;
    Object* target = nullptr;
    if (args->op_type == H5VL_LINK_CREATE_HARD)
    {
        auto found =
            static_cast<Object*>(source->mdata_obj)
                ->locate(args->args.hard.curr_loc_params);
        if (!found.path.empty() || !found.obj ||
            found.obj->find_root() != parent->find_root())
            return -1;
        target = found.obj;
    }
    else if (args->op_type != H5VL_LINK_CREATE_SOFT)
        return -1;
    if (unwrap(destination))
    {
        auto native_args = *args;
        if (native_args.op_type == H5VL_LINK_CREATE_HARD)
            native_args.args.hard.curr_obj = unwrap(args->args.hard.curr_obj);
        auto result = link_create_trampoline(&native_args, unwrap(obj), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        if (result < 0) return result;
    }
    if (target)
    {
        auto* file = dynamic_cast<File*>(target->find_root());
        auto detached = file ? std::find(file->unlinked.begin(),
                                         file->unlinked.end(), target)
                             : std::vector<Object*>::iterator {};
        bool destination_within_target = false;
        for (auto* owner = path.obj; owner; owner = owner->parent)
            if (owner == target)
            {
                destination_within_target = true;
                break;
            }
        if (file && target->name.empty() && target->parent == nullptr &&
            !destination_within_target && detached != file->unlinked.end())
        {
            file->unlinked.erase(detached);
            target->name = path.path;
            path.obj->add_child(target);
        }
        else
            path.obj->add_child(new HardLink(path.path, target));
    }
    else
        path.obj->add_child(new SoftLink(path.path, args->args.soft.target));
    return 0;
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
    auto* source = static_cast<ObjectPointers*>(src_obj ? src_obj : dst_obj);
    auto* destination = static_cast<ObjectPointers*>(dst_obj ? dst_obj : src_obj);
    if (!source || !destination) return -1;
    if (!destination->writable()) return -1;
    const bool native_source = unwrap(source);
    const bool native_destination = unwrap(destination);
    if (native_source != native_destination) return -1;
    if (native_source)
        return VOLBase::link_copy(unwrap(src_obj), loc_params1, unwrap(dst_obj), loc_params2,
                                  under_vol_id, lcpl_id, lapl_id, dxpl_id, req);


    Object::ObjectPath source_path {nullptr, ""};
    Object::ObjectPath destination_path {nullptr, ""};
    if (source->mdata_obj && destination->mdata_obj)
    {
        source_path = static_cast<Object*>(source->mdata_obj)->locate_link(*loc_params1);
        destination_path = static_cast<Object*>(destination->mdata_obj)->locate_link(*loc_params2);
        if (!source_path.obj || !source_path.path.empty() ||
            !destination_path.obj || destination_path.path.empty() ||
            !destination_path.is_name())
            return -1;
        if (source_path.obj->find_root() !=
                destination_path.obj->find_root() &&
            !dynamic_cast<SoftLink*>(source_path.obj))
            return -1;
    }

    if (!source_path.obj)
        return -1;
    Object* copied = nullptr;
    if (auto* soft = dynamic_cast<SoftLink*>(source_path.obj))
        copied = new SoftLink(destination_path.path, soft->target);
    else if (auto* hard = dynamic_cast<HardLink*>(source_path.obj))
        copied = new HardLink(destination_path.path, hard->target);
    else
        copied = new HardLink(destination_path.path, source_path.obj);
    destination_path.obj->add_child(copied);
    return 0;
}

herr_t
LowFive::MetadataVOL::
link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
        void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id,
        hid_t lapl_id, hid_t dxpl_id, void **req)
{
    auto* source = static_cast<ObjectPointers*>(src_obj ? src_obj : dst_obj);
    auto* destination = static_cast<ObjectPointers*>(dst_obj ? dst_obj : src_obj);
    if (!source || !destination) return -1;
    if (!source->writable() || !destination->writable()) return -1;
    const bool native_source = unwrap(source);
    const bool native_destination = unwrap(destination);
    if (native_source != native_destination) return -1;

    Object::ObjectPath source_path {nullptr, ""};
    Object::ObjectPath destination_path {nullptr, ""};
    if (source->mdata_obj && destination->mdata_obj)
    {
        source_path =
            static_cast<Object*>(source->mdata_obj)->locate_link(*loc_params1);
        destination_path =
            static_cast<Object*>(destination->mdata_obj)->locate_link(*loc_params2);
    }
    const bool tracked =
        source_path.obj && source_path.path.empty() && source_path.obj->parent &&
        destination_path.obj && !destination_path.path.empty() &&
        destination_path.is_name();
    if (tracked)
        for (auto* parent = destination_path.obj; parent;
             parent = parent->parent)
            if (parent == source_path.obj) return -1;


    if (native_source)
    {
        auto result = VOLBase::link_move(
            unwrap(src_obj), loc_params1, unwrap(dst_obj), loc_params2,
            under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
        if (result < 0 || !tracked) return result;
    }
    else
    {
        if (!tracked) return -1;
        if (source_path.obj->find_root() !=
                destination_path.obj->find_root() &&
            !dynamic_cast<SoftLink*>(source_path.obj))
            return -1;
    }

    source_path.obj->remove();
    source_path.obj->name = destination_path.path;
    destination_path.obj->add_child(source_path.obj);
    return 0;
}


herr_t
LowFive::MetadataVOL::link_get(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_link_get_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("link_get: obj = {}, get_type = {}", *obj_, args->op_type);

    herr_t res = 0;
    if (unwrap(obj_))
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: link_get: obj = {} metadata are available but won't be used", *obj_);

        res = VOLBase::link_get(unwrap(obj_), loc_params, under_vol_id, args, dxpl_id, req);
    }
    else if (obj_->mdata_obj)
    {
        auto* mdata_obj = static_cast<Object*>(obj_->mdata_obj);
        if (args->op_type == H5VL_LINK_GET_NAME)
        {
            auto* o = mdata_obj->locate_link(*loc_params).exact();
            if (!o) return -1;
            auto length = o->name.size();
            auto capacity = args->args.get_name.name_size;
            if (args->args.get_name.name && capacity)
            {
                auto copied = std::min(length, capacity - 1);
                memcpy(args->args.get_name.name, o->name.data(), copied);
                args->args.get_name.name[copied] = '\0';
            }
            *args->args.get_name.name_len = length;
        } else
        {
            log->warn("Warning: link_get not implemented in metadata yet");
            throw MetadataError(fmt::format("link_get not implemented in metadata yet"));
        }
    } else
        throw MetadataError(fmt::format("link_get(): either passthru or metadata must be specified"));

    return res;
}

// helper function for link_specific()
herr_t
LowFive::MetadataVOL::
link_iter(void *obj, H5VL_link_specific_args_t* args)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    auto* metadata = static_cast<Object*>(pointers->mdata_obj);
    if (!metadata || args->args.iterate.recursive) return -1;
    auto entries = metadata->links(args->args.iterate.idx_type, args->args.iterate.order);
    hsize_t index = args->args.iterate.idx_p ? *args->args.iterate.idx_p : 0;
    if (index > entries.size()) return -1;
    auto* temporary = wrap(nullptr);
    temporary->mdata_obj = metadata;
    temporary->tmp = true;
    temporary->access_flags = static_cast<ObjectPointers*>(obj)->access_flags;
    hid_t id = H5VLwrap_register(temporary, get_identifier_type(metadata));
    if (id < 0) { drop(temporary); return -1; }
    herr_t result = 0;
    for (; index < entries.size(); ++index)
    {
        auto* entry = entries[index];
        H5L_info2_t info{};
        info.cset = H5T_CSET_ASCII;
        info.type = entry->type == ObjectType::SoftLink ? H5L_TYPE_SOFT : H5L_TYPE_HARD;
        if (auto* soft = dynamic_cast<SoftLink*>(entry)) info.u.val_size = soft->target.size() + 1;
        else
        {
            unsigned budget = 64;
            auto* target = entry->resolve_link(budget);
            if (!target) { result = -1; break; }
            target->fill_token(info.u.token);
        }
        result = args->args.iterate.op(id, entry->name.c_str(), &info, args->args.iterate.op_data);
        if (args->args.iterate.idx_p) *args->args.iterate.idx_p = index + 1;
        if (result != 0) break;
    }
    auto closed = H5Idec_ref(id);
    return closed < 0 ? -1 : result;
}

herr_t
LowFive::MetadataVOL::link_specific(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_link_specific_args_t* args, hid_t dxpl_id, void** req)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    if (args->op_type == H5VL_LINK_DELETE && !pointers->writable()) return -1;
    auto* metadata = static_cast<Object*>(static_cast<ObjectPointers*>(obj)->mdata_obj);
    Object* entry = nullptr;
    if (args->op_type == H5VL_LINK_DELETE && metadata)
    {
        if (unwrap(obj) && loc_params->type == H5VL_OBJECT_BY_IDX)
        {
            H5VL_link_get_args_t get_name{};
            get_name.op_type = H5VL_LINK_GET_NAME;
            size_t length = 0;
            get_name.args.get_name.name_len = &length;
            if (VOLBase::link_get(unwrap(obj), loc_params, under_vol_id, &get_name,
                                  dxpl_id, nullptr) < 0)
                return -1;
            std::vector<char> name(length + 1);
            get_name.args.get_name.name_size = name.size();
            get_name.args.get_name.name = name.data();
            if (VOLBase::link_get(unwrap(obj), loc_params, under_vol_id, &get_name,
                                  dxpl_id, nullptr) < 0)
                return -1;

            const auto& by = loc_params->loc_data.loc_by_idx;
            auto owner_path = metadata->search(by.name);
            if (owner_path.obj && owner_path.path.empty())
                for (auto* child : owner_path.obj->children)
                    if (child->type != ObjectType::Attribute &&
                        child->name == std::string(name.data(), length))
                    {
                        entry = child;
                        break;
                    }
        }
        else
        {
            auto path = metadata->locate_link(*loc_params);
            if (path.obj && path.path.empty() && path.obj->parent)
                entry = path.obj;
        }
    }
    if (unwrap(obj))
    {
        auto result = VOLBase::link_specific(unwrap(obj), loc_params, under_vol_id,
                                             args, dxpl_id, req);
        if (result >= 0 && args->op_type == H5VL_LINK_DELETE && entry)
            unlink_metadata(*this, entry);
        return result;
    }
    if (args->op_type == H5VL_LINK_DELETE)
    {
        if (!entry) return -1;
        unlink_metadata(*this, entry);
        return 0;
    }
    if (!metadata) return -1;
    if (args->op_type == H5VL_LINK_EXISTS)
    {
        auto path = metadata->locate_link(*loc_params);
        *args->args.exists.exists = path.obj && path.path.empty();
        return 0;
    }
    if (args->op_type == H5VL_LINK_ITER)
    {
        auto* owner = metadata->locate(*loc_params).exact();
        if (!owner) return -1;
        ObjectPointers temporary = *static_cast<ObjectPointers*>(obj);
        temporary.mdata_obj = owner;
        return link_iter(&temporary, args);
    }
    return -1;
}

herr_t
LowFive::MetadataVOL::link_optional(void* obj, const H5VL_loc_params_t* loc_params, hid_t under_vol_id, H5VL_optional_args_t* args, hid_t dxpl_id, void** req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto log = get_logger();
    log->trace("link_optional: obj = {}, optional_type = {}", *obj_, args->op_type);

    herr_t res = 0;
    if (unwrap(obj_))
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: link_optional: obj = {} metadata are available but won't be used", *obj_);

        res = VOLBase::link_optional(unwrap(obj_), loc_params, under_vol_id, args, dxpl_id, req);
    }
    else if (obj_->mdata_obj)
        log->warn("Warning: link_optional not implemented in metadata yet");
    else
        throw MetadataError(fmt::format("link_optional(): either passthru or metadata must be specified"));

    return res;
}
