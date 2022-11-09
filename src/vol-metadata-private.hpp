#pragma once

#include "metadata.hpp"
#include "metadata/serialization.hpp"

namespace LowFive
{

struct ObjectPointers
{
    void*           h5_obj = nullptr;             // HDF5 object (e.g., dset)
    void*           mdata_obj = nullptr;          // metadata object (tree node)
    bool            tmp = false;

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
    else if (o->type == ObjectType::NamedDtype)
        return H5I_DATATYPE;
    else
        throw MetadataError("cannot identify object type");
}

// using different names to avoid confusion
void serialize(diy::BinaryBuffer& bb, Object* o);
Object* deserialize(diy::BinaryBuffer& bb);

}
