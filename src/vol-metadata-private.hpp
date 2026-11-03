#pragma once

#include "metadata.hpp"
#include "metadata/serialization.hpp"

#include <lowfive/vol-metadata.hpp>
#include <optional>

namespace LowFive
{

struct ObjectPointers
{
    void*           h5_obj = nullptr;             // HDF5 object (e.g., dset)
    void*           mdata_obj = nullptr;          // metadata object (tree node)
    bool            tmp = false;
    bool            file_wrapper = false;
    bool            primary_file_wrapper = false;
    bool            reference_file_wrapper = false;
    unsigned        access_flags = H5F_ACC_RDWR;
    // State inherited by objects opened through the same file handle.
    uint64_t file_handle = 0;
    Hid             fapl {0, true};

    void inherit_file_state(const ObjectPointers& source)
    {
        access_flags = source.access_flags;
        file_handle = source.file_handle;
        fapl = source.fapl;
    }

    bool writable() const { return (access_flags & H5F_ACC_RDWR) != 0; }

    ObjectPointers() = default;
    ObjectPointers(void* h5_obj_): h5_obj(h5_obj_)     {}

    friend
    std::ostream&   operator<<(std::ostream& out, const ObjectPointers& obj)
    {
        fmt::print(out, "[{}: h5 = {}, mdata = {}, tmp = {}", fmt::ptr(&obj), fmt::ptr(obj.h5_obj), fmt::ptr(obj.mdata_obj), obj.tmp);
        if (obj.mdata_obj)
            fmt::print(out, ", name = {}", static_cast<Object*>(obj.mdata_obj)->name);
        fmt::print(out, "]");
        return out;
    }
};

inline H5I_type_t   get_identifier_type(Object* o)
{
    if (o->type == ObjectType::File)
        return H5I_FILE;
    else if (o->type == ObjectType::Group)
        return H5I_GROUP;
    else if (o->type == ObjectType::Dataset)
        return H5I_DATASET;
    else if (o->type == ObjectType::Attribute)
        return H5I_ATTR;
    else if (o->type == ObjectType::NamedDtype ||
             o->type == ObjectType::CommittedDatatype)
        return H5I_DATATYPE;
    else
        throw MetadataError("cannot identify object type");
}

struct DatasetMetadata
{
    Hid type {0, true};
    Hid space {0, true};
    Hid dcpl {0, true};
    Hid dapl {0, true};
};

template<class Getter>
std::optional<DatasetMetadata> query_dataset_metadata(Getter&& get)
{
    H5VL_dataset_get_args_t type_args {};
    type_args.op_type = H5VL_DATASET_GET_TYPE;
    H5VL_dataset_get_args_t space_args {};
    space_args.op_type = H5VL_DATASET_GET_SPACE;
    H5VL_dataset_get_args_t dcpl_args {};
    dcpl_args.op_type = H5VL_DATASET_GET_DCPL;
    H5VL_dataset_get_args_t dapl_args {};
    dapl_args.op_type = H5VL_DATASET_GET_DAPL;
    DatasetMetadata result;
    if (get(type_args) < 0) return std::nullopt;
    result.type.id = type_args.args.get_type.type_id;
    if (get(space_args) < 0) return std::nullopt;
    result.space.id = space_args.args.get_space.space_id;
    if (get(dcpl_args) < 0) return std::nullopt;
    result.dcpl.id = dcpl_args.args.get_dcpl.dcpl_id;
    if (get(dapl_args) < 0) return std::nullopt;
    result.dapl.id = dapl_args.args.get_dapl.dapl_id;
    return result;
}

Object::ObjectPath metadata_target_path(Object* parent,
                                        const std::string& path);

inline Object::ObjectPath metadata_creation_path(Object* parent,
                                                  std::string path,
                                                  bool native)
{
    while (!path.empty() && path.back() == '/') path.pop_back();
    if (path == "." || (path.size() >= 2 &&
                         path.compare(path.size() - 2, 2, "/.") == 0))
        return {nullptr, ""};
    unsigned budget = 64;
    if (!native) return parent->search_path(std::move(path), budget, false);
    auto result = parent->search_path(path, budget, false);
    if (result.obj) return result;

    // A native soft-link ancestor may point to an uncached group. Expand
    // only the parent path; an existing final destination link must still
    // reject creation, including when its target is missing.
    auto slash = path.find_last_of('/');
    if (slash == std::string::npos) return result;
    auto ancestor = metadata_target_path(parent, path.substr(0, slash + 1));
    if (!ancestor.obj) return ancestor;
    if (!ancestor.path.empty() && ancestor.path.back() != '/')
        ancestor.path += '/';
    ancestor.path += path.substr(slash + 1);
    return ancestor;
}

inline Object::ObjectPath metadata_link_creation_path(
    Object* parent, const H5VL_loc_params_t& loc, bool native)
{
    if (loc.type == H5VL_OBJECT_BY_NAME)
        return metadata_creation_path(
            parent, loc.loc_data.loc_by_name.name, native);
    return parent->locate_link(loc);
}

inline bool metadata_creation_allowed(const Object::ObjectPath& path,
                                      hid_t lcpl_id)
{
    if (!path.obj || path.path.empty()) return false;
    if (path.is_name()) return true;
    unsigned create = 0;
    return lcpl_id != H5P_DEFAULT &&
        H5Pget_create_intermediate_group(lcpl_id, &create) >= 0 && create;
}

// Call only after validation or successful native creation. Existing native
// ancestors may be uncached and need their actual properties, not defaults.
inline Object::ObjectPath metadata_create_parents(
    MetadataVOL& vol, Object::ObjectPath path, ObjectPointers* location,
    bool native, hid_t dxpl_id)
{
    while (true)
    {
        auto slash = path.path.find('/');
        if (slash == std::string::npos) return path;
        auto component = path.path.substr(0, slash);
        if (!component.empty() && component != ".")
        {
            Hid gcpl(H5P_DEFAULT);
            if (native)
            {
                H5VL_loc_params_t loc {};
                loc.obj_type = get_identifier_type(
                    static_cast<Object*>(location->mdata_obj));
                loc.type = H5VL_OBJECT_BY_SELF;
                auto fullpath = path.obj->fullname(component).second;
                auto* group = vol.VOLBase::group_open(
                    vol.unwrap(location), &loc, fullpath.c_str(),
                    H5P_DEFAULT, dxpl_id, nullptr);
                if (!group) return {nullptr, ""};
                H5VL_group_get_args_t get {};
                get.op_type = H5VL_GROUP_GET_GCPL;
                auto status = vol.VOLBase::group_get(
                    group, &get, dxpl_id, nullptr);
                vol.VOLBase::group_close(group, dxpl_id, nullptr);
                if (status < 0) return {nullptr, ""};
                gcpl = Hid(get.args.get_gcpl.gcpl_id, true);
            }
            path.obj = path.obj->add_child(new Group(component, gcpl.id));
        }
        path.path.erase(0, slash + 1);
    }
}
Object* replace_metadata(MetadataVOL& vol, File& file, Object* previous,
                         Object* replacement);

// using different names to avoid confusion
void serialize(diy::MemoryBuffer& bb, Object* o, MetadataVOL& vol, bool include_data = false);
Object* deserialize(diy::MemoryBuffer& bb, MetadataVOL& vol);
void serialize_values(diy::MemoryBuffer& bb, const Datatype& type,
                      size_t count, const void* values,
                      File::References* references = nullptr);
void deserialize_values(diy::MemoryBuffer& bb, const Datatype& type,
                        size_t count, void* values,
                        File::References* references = nullptr);
void unlink_metadata(MetadataVOL& vol, Object* object);
File::PendingReference describe_reference(H5R_ref_t& reference,
                                          MetadataVOL* vol = nullptr,
                                          File* source = nullptr);

}
