#pragma once

#include <fmt/core.h>
#include <hdf5.h>

namespace LowFive
{

enum class ObjectType
{
    File,
    Group,
    Dataset,
    NamedDtype
};

struct Dataspace
{
    int                             dim;
    std::vector<size_t>             min;
    std::vector<size_t>             max;

            Dataspace(hid_t space_id)
    {
        dim = H5Sget_simple_extent_ndims(space_id);
        min.resize(dim);
        max.resize(dim);

        // TODO: add a check that the dataspace is simple

        std::vector<hsize_t> min_(dim), max_(dim);
        H5Sget_select_bounds(space_id, min_.data(), max_.data());

        for (size_t i = 0; i < dim; ++i)
        {
            min[i] = min_[i];
            max[i] = max_[i];
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const Dataspace& ds)
    {
        fmt::print(out, "(min = [{}], max = [{}])", fmt::join(ds.min, ","), fmt::join(ds.max, ","));
        return out;
    }
};

struct Datatype
{
    HighFive::DataTypeClass         dtype_class;
    size_t                          dtype_size;         // in bits
    hid_t                           dtype_id;
};

struct Attribute
{
    void*                           value;
    Dataspace                       dataspace;
    Datatype                        datatype;
};

struct Object
{
    using Attributes = std::map<std::string, Attribute>;
    using Properties = std::map<std::string, Attribute>;

    Object*                         parent;
    std::vector<Object*>            children;

    ObjectType                      type;
    std::string                     name;
    Attributes                      attributes;
    Properties                      properties;

    Object(ObjectType type_, std::string name_) :
        parent(nullptr),
        type(type_), name(name_)                        {}

    virtual ~Object()
    {
        // this assumes that there are no cycles, might not be possible, if we implement HDF5 links
        // might want to switch to shared_ptr<Object> instead
        for (auto* child : children)
            delete child;
    }

    virtual void print()
    {
        fmt::print(stderr, "object type = {} name = {}\n", type, name);
        // TODO: print attributes and properties

        for (auto* child : children)
            child->print();
    }

    Object* add_child(Object* object)
    {
        children.push_back(object);
        object->parent = this;
        return object;

        // debug
        fmt::print(stderr, "Added metadata node: name {} type {}\n", object->name, object->type);
    }

    // remove this from parent's children
    void remove()
    {
        if (parent)
            parent->children.erase(std::find(parent->children.begin(), parent->children.end(), this));
    }
};

struct Group : public Object
{
    Group(std::string name) :
        Object(ObjectType::Group, name)                 {}

    void print() override
    {
        fmt::print(stderr, "---- Group ----\n");
        Object::print();
        // TODO: print group
    }
};

struct NamedDtype : public Object
{
    Datatype                        datatype;

    NamedDtype(std::string name) :
        Object(ObjectType::NamedDtype, name)            {}

    void print() override
    {
        fmt::print(stderr, "-- NamedDtype --\n");
        Object::print();
        // TODO: print named datatype
    }
};

struct Dataset : public Object
{
    struct DataTriple
    {
        Dataspace memory;
        Dataspace file;
        const void* data;
    };

    Datatype                        datatype;
    std::vector<DataTriple>         data;


    Dataset(std::string name, hid_t dtype_id) :
        Object(ObjectType::Dataset, name)
    {
        datatype.dtype_id       = dtype_id;
        datatype.dtype_class    = HighFive::convert_type_class(H5Tget_class(dtype_id));
        datatype.dtype_size     = 8 * H5Tget_size(dtype_id);
    }

    void write(Dataspace memory, Dataspace file, const void* buf)
    {
        data.emplace_back(DataTriple { memory, file, buf });
    }

    void print() override
    {
        fmt::print(stderr, "---- Dataset ---\n");
        Object::print();
        auto class_string = HighFive::type_class_string(datatype.dtype_class);
        fmt::print(stderr, "datatype = {}{}\n", class_string, datatype.dtype_size);
        for (auto& d : data)
            fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
    }
};

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    File(std::string filename_):
        Object(ObjectType::File, filename_)
    {
        // debug
        fmt::print(stderr, "Creating metadata for {}\n", filename_);
    }

    // TODO: add find(name), add(name, object)

    // preorder depth-first traversal
    void print_tree()
    {
        fmt::print(stderr, "Printing metadata tree for {}\n", name);
        Object::print();
    }
};

}
