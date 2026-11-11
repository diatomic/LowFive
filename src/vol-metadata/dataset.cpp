#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"
#include "../vol-metadata-private.hpp"
#include <cassert>

namespace
{
struct UnderConnector
{
    hid_t connector = H5I_INVALID_HID;
    void* info = nullptr;

    explicit UnderConnector(hid_t fapl)
    {
        if (H5Pget_vol_id(fapl, &connector) < 0) return;
        void* value = nullptr;
        if (H5Pget_vol_info(fapl, &value) < 0)
        {
            H5Idec_ref(connector);
            connector = H5I_INVALID_HID;
            return;
        }
        info = value;
    }

    ~UnderConnector()
    {
        if (info)
            H5VLfree_connector_info(connector, info);
        if (connector >= 0)
            H5Idec_ref(connector);
    }

    explicit operator bool() const
    {
        return connector >= 0;
    }
};

// HDF5 property-list copies borrow modern references. Keep an independent
// materialized fill alive until the last returned property-list copy closes.
struct PropertyFill
{
    LowFive::Datatype type;
    std::unique_ptr<char[]> values;
    size_t users = 1;

    explicit PropertyFill(const LowFive::Datatype& type_):
        type(type_), values(new char[type.dtype_size]()) {}

    ~PropertyFill()
    {
        type.destroy_values(1, values.get());
    }

    static herr_t copy(const char*, size_t, void* value) noexcept
    {
        ++(*static_cast<PropertyFill**>(value))->users;
        return 0;
    }

    static herr_t close(const char*, size_t, void* value) noexcept
    {
        auto* fill = *static_cast<PropertyFill**>(value);
        if (--fill->users == 0) delete fill;
        return 0;
    }

    static herr_t remove(hid_t, const char* name, size_t size, void* value) noexcept
    {
        return close(name, size, value);
    }
};

void copy_fill_properties(LowFive::MetadataVOL& vol, LowFive::Dataset& dataset,
                          hid_t properties)
{
    if (!dataset.fill_data) return;
    auto* file = dynamic_cast<LowFive::File*>(dataset.find_root());
    std::vector<const LowFive::File::PendingReference*> pending;
    dataset.type.visit_references(
        1, dataset.fill_data.get(), [&](H5R_ref_t* reference)
        {
            const auto found = file->references.find(reference);
            pending.push_back(found == file->references.end() ? nullptr :
                                                                &found->second);
        });
    if (pending.empty())
    {
        if (H5Pset_fill_value(properties, dataset.type.id,
                              dataset.fill_data.get()) < 0)
            throw LowFive::MetadataError("Could not copy dataset fill value");
        return;
    }

    auto fill = std::make_unique<PropertyFill>(dataset.type);
    dataset.type.copy_values(1, dataset.fill_data.get(), fill->values.get());
    size_t index = 0;
    dataset.type.visit_references(
        1, fill->values.get(), [&](H5R_ref_t* reference)
        {
            const auto* state = pending[index++];
            if (!state || state->resolved) return;
            file->references.emplace(reference, *state);
            try
            {
                vol.resolve_reference(*file, reference);
            }
            catch (...)
            {
                file->references.erase(reference);
                throw;
            }
            file->references.erase(reference);
        });
    auto* owner = fill.get();
    if (H5Pinsert2(properties, "lowfive.fill-owner", sizeof(owner), &owner,
                   nullptr, nullptr, PropertyFill::remove, PropertyFill::copy,
                   nullptr, PropertyFill::close) < 0)
        throw LowFive::MetadataError("Could not retain dataset fill references");
    fill.release();
    if (H5Pset_fill_value(properties, dataset.type.id, owner->values.get()) < 0)
        throw LowFive::MetadataError("Could not copy dataset fill value");
}
template<class Operation>
herr_t with_native_dataset(LowFive::Dataset& dataset, hid_t fapl,
                           void* native_dataset, void* native_file,
                           hid_t dxpl_id, Operation&& operation)
{
    if (!dynamic_cast<LowFive::File*>(dataset.find_root())) return -1;
    UnderConnector configured(fapl);
    if (!configured) return -1;

    if (native_dataset)
        return operation(native_dataset, configured.connector) < 0 ? -1 : 0;

    if (!native_file) return -1;

    H5VL_loc_params_t loc_params {};
    loc_params.type = H5VL_OBJECT_BY_TOKEN;
    loc_params.obj_type = H5I_FILE;
    H5O_token_t token {};
    dataset.fill_token(token);
    loc_params.loc_data.loc_by_token.token = &token;
    H5I_type_t opened_type = H5I_BADID;
    native_dataset =
        H5VLobject_open(native_file, &loc_params, configured.connector,
                        &opened_type, dxpl_id, nullptr);
    if (!native_dataset || opened_type != H5I_DATASET)
    {
        if (native_dataset)
            H5VLdataset_close(native_dataset, configured.connector, dxpl_id,
                              nullptr);
        return -1;
    }

    const auto result = operation(native_dataset, configured.connector);
    const auto dataset_close =
        H5VLdataset_close(native_dataset, configured.connector, dxpl_id,
                          nullptr);
    return result < 0 || dataset_close < 0 ? -1 : 0;
}
std::pair<void*, void*> native_objects(LowFive::MetadataVOL& vol,
                                       LowFive::ObjectPointers* current,
                                       LowFive::Dataset& dataset)
{
    void* native_dataset = nullptr;
    void* native_file = nullptr;
    auto* file = dataset.find_root();
    for (auto* pointer : vol.our_objects)
    {
        auto* wrapped = static_cast<LowFive::ObjectPointers*>(pointer);
        if (wrapped == current || !vol.unwrap(wrapped)) continue;
        if (wrapped->mdata_obj == &dataset)
            native_dataset = vol.unwrap(wrapped);
        else if (wrapped->file_wrapper && wrapped->mdata_obj == file)
            native_file = vol.unwrap(wrapped);
    }
    if (!native_file)
        if (auto* metadata = dynamic_cast<LowFive::File*>(file))
            if (metadata->reference_file_id >= 0)
                native_file = H5VLobject(metadata->reference_file_id);
    return {native_dataset, native_file};
}

}

void*
LowFive::MetadataVOL::
dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
        const char *name,
        hid_t lcpl_id,  hid_t type_id,
        hid_t space_id, hid_t dcpl_id,
        hid_t dapl_id,  hid_t dxpl_id, void **req)
{
    auto* parent_pointers = static_cast<ObjectPointers*>(obj);
    if (!parent_pointers->writable()) return nullptr;
    auto* parent = static_cast<Object*>(static_cast<ObjectPointers*>(obj)->mdata_obj);
    std::string dataset_name = name ? name : "";
    auto filepath = parent->fullname(dataset_name);
    bool native = match_any(filepath, passthru);
    bool in_memory = match_any(filepath, memory);
    auto path = metadata_creation_path(parent, dataset_name, native);
    if (!path.obj || (name && path.path.empty())) return nullptr;
    if (name && !native && !metadata_creation_allowed(path, lcpl_id))
        return nullptr;
    void* underlying = nullptr;
    if (native)
    {
        if (!unwrap(obj)) return nullptr;
        underlying = VOLBase::dataset_create(unwrap(obj), loc_params, name, lcpl_id, type_id, space_id, dcpl_id, dapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    if (name)
    {
        path = metadata_create_parents(*this, std::move(path), parent_pointers,
                                       native, dxpl_id);
        if (!path.obj)
        {
            if (underlying) VOLBase::dataset_close(underlying, dxpl_id, nullptr);
            return nullptr;
        }
    }
    auto ownership = match_any(filepath, zerocopy) ? Dataset::Ownership::user : Dataset::Ownership::lowfive;
    auto* metadata = new Dataset(path.path, type_id, space_id, ownership, dcpl_id,
                                 dapl_id, native, in_memory);
    if (name)
        metadata = static_cast<Dataset*>(path.obj->add_child(metadata));
    else if (auto* file = dynamic_cast<File*>(parent->find_root()))
    {
        metadata->set_root(file);
        file->unlinked.push_back(metadata);
    }
    else
        metadata = static_cast<Dataset*>(path.obj->add_child(metadata));
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->inherit_file_state(*parent_pointers);
    return result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    auto* parent = static_cast<Object*>(static_cast<ObjectPointers*>(obj)->mdata_obj);
    auto path = parent->search(name);
    void* underlying = nullptr;
    if (match_any(parent->fullname(name), passthru) && unwrap(obj))
    {
        underlying = VOLBase::dataset_open(unwrap(obj), loc_params, name, dapl_id, dxpl_id, req);
        if (!underlying) return nullptr;
    }
    Object* metadata = path.path.empty() ? path.obj : nullptr;
    if (metadata && metadata->type != ObjectType::Dataset)
    {
        if (underlying) VOLBase::dataset_close(underlying, dxpl_id, req);
        return nullptr;
    }
    if (!metadata)
    {
        if (!underlying) return nullptr;
        auto properties = query_dataset_metadata(
            [&](H5VL_dataset_get_args_t& args)
            {
                return VOLBase::dataset_get(
                    underlying, &args, dxpl_id, nullptr);
            });
        if (!properties)
        {
            VOLBase::dataset_close(underlying, dxpl_id, nullptr);
            return nullptr;
        }
        auto* inserted = new Dataset(
            "", properties->type.id, properties->space.id,
            Dataset::Ownership::lowfive, properties->dcpl,
            properties->dapl, true, false);
        auto target = metadata_target_path(parent, name);
        if (target.obj && !target.path.empty())
            metadata = target.obj->add_path(inserted, target.path);
        else
            delete inserted;
        if (!metadata)
        {
            VOLBase::dataset_close(underlying, dxpl_id, nullptr);
            return nullptr;
        }
    }
    auto* result = wrap(underlying);
    result->mdata_obj = metadata;
    result->inherit_file_state(*static_cast<ObjectPointers*>(obj));
    return result;
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto log = get_logger();
    log->trace("dataset_close: dset = {}, dxpl_id = {}", *dset_, dxpl_id);
    if (dset_->tmp)
    {
        log->trace("temporary reference, skipping close");
        return 0;
    }

    herr_t retval = 0;

    // close the file, keep in-memory structures in the hierarchy (they may be needed up to file_close())
    if (unwrap(dset_))
        retval = VOLBase::dataset_close(unwrap(dset_), dxpl_id, req);

    // TODO: not sure what the point of this was; the comment above offers a clue, but I still don't understand the logic
    //else if (vol_properties.passthru && vol_properties.memory)
    //{
    //    if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))
    //        retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    //}

    return retval;
}
herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto log = get_logger();
    log->trace("dataset_read: dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}",
            *dset_,
            mem_space_id, Dataspace(mem_space_id),
            file_space_id, Dataspace(file_space_id));

    if (unwrap(dset_))
    {
        // warn if metadata are available but won't be used
        if (dset_->mdata_obj)
            log->warn("Warning: dataset_read: obj = {} metadata are available but won't be used", *dset_);

        return VOLBase::dataset_read(unwrap(dset_), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    }
    else if (dset_->mdata_obj)
    {
        auto* ds = dynamic_cast<Dataset*>(static_cast<Object*>(dset_->mdata_obj));
        if (!ds) return -1;
        if (ds->is_passthru)
        {
            auto native = native_objects(*this, dset_, *ds);
            return with_native_dataset(
                *ds, dset_->fapl.id, native.first, native.second, plist_id,
                [&](void* native, hid_t connector)
                {
                    return H5VLdataset_read(
                        1, &native, connector, &mem_type_id, &mem_space_id,
                        &file_space_id, plist_id, &buf, nullptr);
                });
        }
        resolve_references(ds, mem_space_id, file_space_id);
        try
        {
            if (file_space_id != H5S_ALL && Dataspace(file_space_id).dim != ds->space.dim)
                throw MetadataError(fmt::format("Error: dataset_read(): dim mismatch"));
            ds->read(mem_type_id, mem_space_id, file_space_id, buf);
        }
        catch (...)
        {
            suspend_references(ds);
            throw;
        }
        suspend_references(ds);
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    auto* pointers = static_cast<ObjectPointers*>(dset);
    if (!pointers->writable()) return -1;
    if (unwrap(dset))
    {
        herr_t result = VOLBase::dataset_write(unwrap(dset), mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
        if (result < 0) return result;
    }
    auto* metadata = dynamic_cast<Dataset*>(static_cast<Object*>(pointers->mdata_obj));
    bool wrote_native = false;
    if (!unwrap(dset) && metadata && metadata->is_passthru)
    {
        auto native = native_objects(*this, pointers, *metadata);
        if (with_native_dataset(
                *metadata, pointers->fapl.id, native.first, native.second,
                plist_id,
                [&](void* native, hid_t connector)
                {
                    const void* output = buf;
                    return H5VLdataset_write(
                        1, &native, connector, &mem_type_id, &mem_space_id,
                        &file_space_id, plist_id, &output, nullptr);
                }) < 0)
            return -1;
        wrote_native = true;
    }
    if (metadata && metadata->is_memory)
    {
        try
        {
            Datatype memory_type(mem_type_id);
            if (memory_type.needs_background(metadata->type))
                resolve_references(metadata, mem_space_id, file_space_id);
            metadata->write(memory_type, Dataspace(mem_space_id),
                            Dataspace(file_space_id), buf);
        }
        catch (...)
        {
            suspend_references(metadata);
            throw;
        }
        suspend_references(metadata);
    }
    else if (!unwrap(dset) && !wrote_native) return -1;
    if (after_dataset_write) after_dataset_write();
    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_specific(void *obj, H5VL_dataset_specific_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;
    if (args->op_type == H5VL_DATASET_SET_EXTENT && !obj_->writable())
        return -1;

    auto specific_type = args->op_type;

    auto log = get_logger();
    log->trace("dataset_specific: dset = {} specific_type = {}", *obj_, specific_type);

    herr_t res = 0;
    if (unwrap(obj_))
        res = VOLBase::dataset_specific(unwrap(obj_), args, dxpl_id, req);
    if (res < 0) return res;

    if (auto* dataset = dynamic_cast<Dataset*>(
            static_cast<Object*>(obj_->mdata_obj)))
    {
        // specific types are enumerated in H5VLconnector.h
        switch (specific_type)
        {
        case H5VL_DATASET_SET_EXTENT:
        {
            // adapted from H5VLnative_dataset.c, H5VL__native_dataset_specific()
            const hsize_t *size = args->args.set_extent.size;
            dataset->set_extent(size);
            break;
        }
        case H5VL_DATASET_FLUSH:
            log->trace("dataset_specific specific_type H5VL_DATASET_FLUSH is a no-op in metadata");
            break;
        case H5VL_DATASET_REFRESH:
            log->trace("dataset_specific specific_type H5VL_DATASET_REFRESH is a no-op in metadata");
            break;
        default:
            if (!unwrap(obj_)) return -1;
            break;
        }
    }
    else if (!unwrap(obj_))
        throw MetadataError(fmt::format("dataset_specific(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
dataset_optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*)obj;

    auto opt_type = args->op_type;

    auto log = get_logger();
    log->trace("dataset_optional: dset = {} optional_type = {}", *obj_, opt_type);

    herr_t res = 0;
    if (unwrap(obj_))
    {
        // warn if metadata are available but won't be used
        if (obj_->mdata_obj)
            log->warn("Warning: dataset_optional: obj = {} metadata are available but won't be used", *obj_);

        res = VOLBase::dataset_optional(unwrap(obj_), args, dxpl_id, req);
    }
    else if (obj_->mdata_obj)
    {
        return -1;
    }
    else
        throw MetadataError(fmt::format("dataset_optional(): either passthru or metadata must be specified"));

    return res;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_args_t* args, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    auto get_type = args->op_type;

    auto log = get_logger();
    log->trace("dset = {}, get_type = {}, req = {}", *dset_, get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    // TODO: Why do we prefer passthru to memory here? Only reason is that it's
    //       more complete. But perhaps it's best to trigger the error in the else
    //       than survive on passthru?
    herr_t result = 0;
    if (unwrap(dset_))
    {
        // warn if metadata are available but won't be used
        if (dset_->mdata_obj)
            log->warn("Warning: dataset_get: obj = {} metadata are available but won't be used", *dset_);

        result = VOLBase::dataset_get(unwrap(dset_), args, dxpl_id, req);
    }
    else if (dset_->mdata_obj)
    {                                                       // see hdf5 H5VLnative_dataset.c, H5VL__native_dataset_get()
        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            log->trace("GET_SPACE");
            auto& dataspace = static_cast<Dataset*>(dset_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            log->trace("copied space id = {}, space = {}", space_id, Dataspace(space_id));

            args->args.get_space.space_id = space_id;
            log->trace("arguments = {} -> {}", "args->args.get_space.space_id", args->args.get_space.space_id);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            log->trace("GET_TYPE");
            auto& datatype = static_cast<Dataset*>(dset_->mdata_obj)->type;

            log->trace("dataset data type id = {}, datatype = {}",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            log->trace("copied data type id = {}, datatype = {}",
                    dtype_id, Datatype(dtype_id));

            args->args.get_type.type_id = dtype_id;
            log->trace("arguments = {} -> {}", "args->args.get_type.type_id", args->args.get_type.type_id);
        } else if (get_type == H5VL_DATASET_GET_DCPL)
        {
            log->trace("GET_DCPL");
            auto* dset = static_cast<Dataset*>(dset_->mdata_obj);
            Hid properties(H5Pcopy(dset->dcpl.id), true);
            if (properties.id < 0)
                throw MetadataError("Could not copy dataset creation properties");
            copy_fill_properties(*this, *dset, properties.id);
            args->args.get_dcpl.dcpl_id = std::exchange(properties.id, 0);
            log->trace("arguments = {} -> {}", "args->args.get_dcpl.dcpl_id", args->args.get_dcpl.dcpl_id);
        } else if (get_type == H5VL_DATASET_GET_DAPL)
        {
            log->trace("GET_DAPL");
            auto* dset = static_cast<Dataset*>(dset_->mdata_obj);
            args->args.get_dapl.dapl_id = H5Pcopy(dset->dapl.id);
            log->trace("arguments = {} -> {}", "args->args.get_dapl.dapl_id", args->args.get_dapl.dapl_id);
        } else
        {
            throw MetadataError(fmt::format("Unknown get_type == {} in dataset_get()", get_type));
        }
    }

    return result;
} // end dataset_get
