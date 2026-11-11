#include <lowfive/vol-metadata.hpp>
#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <cassert>

namespace
{
LowFive::Object* attribute_owner(LowFive::MetadataVOL& vol, void* obj,
                                const H5VL_loc_params_t* location, hid_t dxpl, void** request)
{
    auto* parent = static_cast<LowFive::Object*>(
        static_cast<LowFive::ObjectPointers*>(obj)->mdata_obj);
    if (parent)
    {
        auto path = parent->locate(*location);
        if (path.obj && path.path.empty()) return path.obj;
    }
    if (!parent) return nullptr;
    if (!vol.unwrap(obj)) return nullptr;
    H5I_type_t type;
    auto* opened = static_cast<LowFive::ObjectPointers*>(vol.MetadataVOL::object_open(obj, location, &type, dxpl, request));
    if (!opened) return nullptr;
    auto* owner = static_cast<LowFive::Object*>(opened->mdata_obj);
    herr_t closed = type == H5I_GROUP ? vol.MetadataVOL::group_close(opened, dxpl, request) :
                    type == H5I_DATASET ? vol.MetadataVOL::dataset_close(opened, dxpl, request) :
                    vol.MetadataVOL::datatype_close(opened, dxpl, request);
    vol.drop(opened);
    return closed < 0 ? nullptr : owner;
}

LowFive::Object* indexed_attribute(LowFive::Object* owner, H5_index_t index,
                                   H5_iter_order_t order, hsize_t position)
{
    std::vector<LowFive::Object*> attributes;
    for (auto* child : owner->children)
        if (child->type == LowFive::ObjectType::Attribute)
            attributes.push_back(child);
    if (index == H5_INDEX_NAME)
        std::sort(attributes.begin(), attributes.end(),
                  [](const auto* lhs, const auto* rhs) { return lhs->name < rhs->name; });
    else if (index != H5_INDEX_CRT_ORDER)
        return nullptr;
    if (order == H5_ITER_DEC)
        std::reverse(attributes.begin(), attributes.end());
    else if (order != H5_ITER_INC && order != H5_ITER_NATIVE)
        return nullptr;
    return position < attributes.size() ? attributes[position] : nullptr;
}
}

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    auto* parent_pointers = static_cast<ObjectPointers*>(obj);
    if (!parent_pointers->writable()) return nullptr;
    auto* owner = attribute_owner(*this, obj, loc_params, dxpl_id, req);
    if (!owner || owner->attribute(name)) return nullptr;
    void* underlying = nullptr;
    if (unwrap(obj) && match_any(owner->fullname(name), passthru))
    {
        underlying = VOLBase::attr_create(unwrap(obj), loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    auto* result = wrap(underlying);
    result->mdata_obj = owner->add_child(new Attribute(name, type_id, space_id));
    result->inherit_file_state(*parent_pointers);
    return result;
}

void*
LowFive::MetadataVOL::
attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    auto* owner = attribute_owner(*this, obj, loc_params, dxpl_id, req);
    void* underlying = nullptr;
    if (unwrap(obj) &&
        (!owner || match_any(owner->fullname(name), passthru)))
    {
        underlying = VOLBase::attr_open(
            unwrap(obj), loc_params, name, aapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    if (!owner)
    {
        if (!underlying) return nullptr;
        auto* result = wrap(underlying);
        result->inherit_file_state(*static_cast<ObjectPointers*>(obj));
        return result;
    }

    Object* metadata = owner->attribute(name);
    if (!metadata)
    {
        if (!underlying) return nullptr;
        metadata = owner->add_child(new DummyObject(ObjectType::Attribute, name));
    }
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->inherit_file_state(*static_cast<ObjectPointers*>(obj));
    return result;
}

herr_t
LowFive::MetadataVOL::
attr_read(void *attr, hid_t mem_type_id, void *buf,
        hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    auto log = get_logger();
    log->trace("Attr Read");
    auto mem_type = Datatype(mem_type_id);
    log->trace("attr = {}, mem_type_id = {}, mem type = {}", *attr_, mem_type_id, mem_type);

    if (unwrap(attr_))
    {
        // warn if metadata are available but won't be used
        if (attr_->mdata_obj)
            log->warn("Warning: attr_read: obj = {} metadata are available but won't be used", *attr_);

        return VOLBase::attr_read(unwrap(attr), mem_type_id, buf, dxpl_id, req);
    }
    else
    {
        log_assert(attr_->mdata_obj, "mdata_obj must be set in metadata mode");
        Attribute* a = (Attribute*) attr_->mdata_obj;

        resolve_references(a);
        try
        {
            log->trace("type = {}, space = {}, mem_type = {}", a->type, a->space, a->mem_type);
            a->read(Datatype(mem_type), buf);
        }
        catch (...)
        {
            suspend_references(a);
            throw;
        }
        suspend_references(a);
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    auto get_type = args->op_type;

    auto log = get_logger();
    log->trace("attr = {}, get_type = {}, req = {}", *obj_, get_type, fmt::ptr(req));

    log->trace("Attr Get");
    log->trace("get type = {}", get_type);

    // TODO: again, why do we prefer passthru?
    if (unwrap(obj_))
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: attr_get: obj = {} metadata are available but won't be used", *obj_);

        return VOLBase::attr_get(unwrap(obj), args, dxpl_id, req);
    }
    else if (obj_->mdata_obj)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            args->args.get_space.space_id = space_id;
            log->trace("arguments = {} -> {}", "args->args.get_space.space_id", args->args.get_space.space_id);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            args->args.get_type.type_id = dtype_id;
            log->trace("arguments = {} -> {}", "args->args.get_type.type_id", args->args.get_type.type_id);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in attr_get()", get_type));
        }
    }

    return 0;
}

// helper function for attr_specific()
htri_t
LowFive::MetadataVOL::
attr_exists(Object *mdata_obj, const char* attr_name, htri_t* ret)
{
    // check direct children of the parent object (NB, not full search all the way down the tree)
    *ret = 0;           // not found
    for (auto& c : mdata_obj->children)
    {
        if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name)
        {
            *ret = 1;   // found
            break;
        }
    }
    auto log = get_logger();
    if (*ret)
        log->trace("Found attribute {} as a child of the parent {}", attr_name, mdata_obj->name);
    else
        log->trace("Did not find attribute {} as a child of the parent {}", attr_name, mdata_obj->name);

    return *ret;
}

herr_t
LowFive::MetadataVOL::
attr_iter(void *obj, const H5VL_loc_params_t *loc_params, H5_index_t index,
          H5_iter_order_t order, hsize_t *idx, H5A_operator2_t op,
          void* op_data)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    auto* metadata = static_cast<Object*>(pointers->mdata_obj);
    try { metadata = metadata->locate(*loc_params).exact(); }
    catch (...) { return -1; }
    if (!metadata) return -1;

    std::vector<std::pair<Object*, int64_t>> attributes;
    int64_t creation_order = 0;
    for (auto* child : metadata->children)
        if (child->type == ObjectType::Attribute)
            attributes.emplace_back(child, creation_order++);
    if (index == H5_INDEX_NAME)
        std::sort(attributes.begin(), attributes.end(),
                  [](const auto& lhs, const auto& rhs)
                  { return lhs.first->name < rhs.first->name; });
    else if (index != H5_INDEX_CRT_ORDER)
        return -1;
    if (order == H5_ITER_DEC)
        std::reverse(attributes.begin(), attributes.end());
    else if (order != H5_ITER_INC && order != H5_ITER_NATIVE)
        return -1;

    auto* temporary = wrap(nullptr);
    temporary->mdata_obj = metadata;
    temporary->tmp = true;
    temporary->inherit_file_state(*pointers);
    hid_t id = H5VLwrap_register(temporary, get_identifier_type(metadata));
    if (id < 0)
    {
        drop(temporary);
        return -1;
    }

    herr_t result = 0;
    const hsize_t start = idx ? *idx : 0;
    for (hsize_t position = start; position < attributes.size(); ++position)
    {
        auto [attribute, corder] = attributes[position];
        auto* value = dynamic_cast<Attribute*>(attribute);
        H5A_info_t info {};
        info.corder_valid = true;
        info.corder = corder;
        info.cset = H5T_CSET_ASCII;
        info.data_size = value ?
            value->space.size() * value->type.dtype_size : 0;
        result = op(id, attribute->name.c_str(), &info, op_data);
        if (idx) *idx = position + 1;
        if (result != 0) break;
    }

    H5Idec_ref(id);
    return result;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    auto* pointers = static_cast<ObjectPointers*>(attr);
    if (!pointers->writable()) return -1;
    if (unwrap(attr))
    {
        herr_t result = VOLBase::attr_write(unwrap(attr), mem_type_id, buf, dxpl_id, req);
        if (result < 0) return result;
    }
    if (auto* metadata = dynamic_cast<Attribute*>(
            static_cast<Object*>(pointers->mdata_obj)))
    {
        resolve_references(metadata);
        try
        {
            metadata->write(Datatype(mem_type_id), buf);
        }
        catch (...)
        {
            suspend_references(metadata);
            throw;
        }
        suspend_references(metadata);
    }
    else if (!unwrap(attr)) return -1;
    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;
    if (attr_->tmp) return 0;

    auto log = get_logger();
    log->trace("Attr Close");
    log->trace("close: {}, dxpl_id = {}", *attr_, dxpl_id);

    herr_t retval = 0;

    if (unwrap(attr_))
        retval = VOLBase::attr_close(unwrap(attr_), dxpl_id, req);

    return retval;
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t* args, hid_t dxpl_id, void **req)
{
    auto* pointers = static_cast<ObjectPointers*>(obj);
    const bool mutating =
        args->op_type == H5VL_ATTR_RENAME ||
        args->op_type == H5VL_ATTR_DELETE ||
        args->op_type == H5VL_ATTR_DELETE_BY_IDX;
    if (mutating && !pointers->writable()) return -1;
    auto* owner = attribute_owner(*this, obj, loc_params, dxpl_id, req);
    if (!owner)
        return unwrap(obj) ?
            VOLBase::attr_specific(unwrap(obj), loc_params, args, dxpl_id, req) :
            -1;
    const char* name = nullptr;
    if (args->op_type == H5VL_ATTR_EXISTS) name = args->args.exists.name;
    else if (args->op_type == H5VL_ATTR_DELETE) name = args->args.del.name;
    else if (args->op_type == H5VL_ATTR_RENAME) name = args->args.rename.old_name;
    const bool native = unwrap(obj) &&
        (args->op_type == H5VL_ATTR_ITER ||
         args->op_type == H5VL_ATTR_DELETE_BY_IDX ||
         (name && match_any(owner->fullname(name), passthru)));
    Object* native_indexed_attribute = nullptr;
    if (native && args->op_type == H5VL_ATTR_DELETE_BY_IDX)
    {
        H5VL_loc_params_t indexed{};
        indexed.obj_type = loc_params->obj_type;
        indexed.type = H5VL_OBJECT_BY_IDX;
        if (loc_params->type == H5VL_OBJECT_BY_SELF)
        {
            indexed.loc_data.loc_by_idx.name = ".";
            indexed.loc_data.loc_by_idx.lapl_id = H5P_DEFAULT;
        }
        else if (loc_params->type == H5VL_OBJECT_BY_NAME)
        {
            indexed.loc_data.loc_by_idx.name = loc_params->loc_data.loc_by_name.name;
            indexed.loc_data.loc_by_idx.lapl_id = loc_params->loc_data.loc_by_name.lapl_id;
        }
        else
            return -1;
        indexed.loc_data.loc_by_idx.idx_type = args->args.delete_by_idx.idx_type;
        indexed.loc_data.loc_by_idx.order = args->args.delete_by_idx.order;
        indexed.loc_data.loc_by_idx.n = args->args.delete_by_idx.n;

        H5VL_attr_get_args_t get_name{};
        get_name.op_type = H5VL_ATTR_GET_NAME;
        get_name.args.get_name.loc_params = indexed;
        size_t length = 0;
        get_name.args.get_name.attr_name_len = &length;
        if (VOLBase::attr_get(unwrap(obj), &get_name, dxpl_id, nullptr) < 0)
            return -1;
        std::vector<char> buffer(length + 1);
        get_name.args.get_name.buf_size = buffer.size();
        get_name.args.get_name.buf = buffer.data();
        if (VOLBase::attr_get(unwrap(obj), &get_name, dxpl_id, nullptr) < 0)
            return -1;
        native_indexed_attribute = owner->attribute(std::string(buffer.data(), length));
    }
    if (native)
    {
        auto result = VOLBase::attr_specific(unwrap(obj), loc_params, args, dxpl_id, req);
        if (result < 0 || args->op_type == H5VL_ATTR_EXISTS ||
            args->op_type == H5VL_ATTR_ITER)
            return result;
    }
    switch (args->op_type)
    {
    case H5VL_ATTR_EXISTS:
        *args->args.exists.exists = owner->attribute(name) != nullptr;
        return 0;
    case H5VL_ATTR_RENAME:
    {
        auto* attribute = owner->attribute(name);
        if (!attribute) return native ? 0 : -1;
        if (attribute->name == args->args.rename.new_name) return 0;
        if (owner->attribute(args->args.rename.new_name)) return -1;
        attribute->name = args->args.rename.new_name;
        return 0;
    }
    case H5VL_ATTR_DELETE:
    {
        auto* attribute = owner->attribute(name);
        if (!attribute) return native ? 0 : -1;
        unlink_metadata(*this, attribute);
        return 0;
    }
    case H5VL_ATTR_DELETE_BY_IDX:
    {
        if (!native)
        {
            auto* attribute = indexed_attribute(owner, args->args.delete_by_idx.idx_type,
                                                args->args.delete_by_idx.order,
                                                args->args.delete_by_idx.n);
            if (!attribute) return -1;
            unlink_metadata(*this, attribute);
            return 0;
        }
        if (native_indexed_attribute)
            unlink_metadata(*this, native_indexed_attribute);
        return 0;
    }
    case H5VL_ATTR_ITER:
        return attr_iter(obj, loc_params, args->args.iterate.idx_type,
                         args->args.iterate.order, args->args.iterate.idx,
                         args->args.iterate.op, args->args.iterate.op_data);
    default:
        return -1;
    }
}

herr_t
LowFive::MetadataVOL::
attr_optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    if (!unwrap(obj)) return -1;
    auto log = get_logger();

    // warn if metadata are available but won't be used
    if (obj_->mdata_obj)
        log->warn("Warning: attr_optional: obj = {} metadata are available but won't be used", *obj_);

    return VOLBase::attr_optional(unwrap(obj), args, dxpl_id, req);
}

