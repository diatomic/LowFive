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
    // Shared by objects opened through the same file handle (H5F_OBJ_LOCAL).
    uint64_t file_handle = 0;

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
