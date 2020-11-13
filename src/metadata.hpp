#pragma once

using namespace std;

#define MAX_DIM 32                                      // this is HDF5's limit

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
    vector<size_t>                  min;
    vector<size_t>                  max;

    void print()
    {
        fmt::print(stderr, "dim = {} ", dim);

        fmt::print(stderr, "min = [ ");
        for (auto i = 0; i < min.size(); i++)
            fmt::print(stderr, "{} ", min[i]);
        fmt::print(stderr, "] ");

        fmt::print(stderr, "max = [ ");
        for (auto i = 0; i < max.size(); i++)
            fmt::print(stderr, "{} ", max[i]);
        fmt::print(stderr, "]\n");
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
    Object*                         parent;
    vector<Object*>                 children;

    ObjectType                      type;
    string                          name;
    map<string, Attribute>          attributes;
    map<string, Attribute>          properties;

    Object(ObjectType type_, string name_) :
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
    Group(string name) :
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

    NamedDtype(string name) :
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
    vector<Dataspace>               m_dataspaces;       // memory dataspaces
    vector<Dataspace>               f_dataspaces;       // file dataspaces
    Datatype                        datatype;
    const void*                     data;

    Dataset(string name, hid_t dtype_id) :
        Object(ObjectType::Dataset, name),
        data(NULL)
    {
        datatype.dtype_id       = dtype_id;
        datatype.dtype_class    = HighFive::convert_type_class(H5Tget_class(dtype_id));
        datatype.dtype_size     = 8 * H5Tget_size(dtype_id);
    }

    void add_dataspace(bool memory,                     // memory or file dataspace
            int             ndim,                       // number of dims
            vector<hsize_t> start,                      // starting dims
            vector<hsize_t> end)                        // ending dims
    {
        Dataspace dataspace;
        dataspace.dim     = ndim;
        dataspace.min.resize(ndim);
        dataspace.max.resize(ndim);
        for (auto i = 0; i < ndim; i++)
        {
            dataspace.min[i] = start[i];
            dataspace.max[i] = end[i];
        }
        if (memory)
            m_dataspaces.push_back(dataspace);
        else
            f_dataspaces.push_back(dataspace);
    }

    void set_data(const void* data_)                    { data = data_; }

    void print() override
    {
        fmt::print(stderr, "---- Dataset ---\n");
        Object::print();
        auto class_string = HighFive::type_class_string(datatype.dtype_class);
        fmt::print(stderr, "datatype = {}{}\n", class_string, datatype.dtype_size);
        for (auto i = 0; i < m_dataspaces.size(); i++)
        {
            fmt::print(stderr, "m_dataspaces[{}]: ", i);
            m_dataspaces[i].print();
        }
        for (auto i = 0; i < f_dataspaces.size(); i++)
        {
            fmt::print(stderr, "f_dataspaces[{}]: ", i);
            f_dataspaces[i].print();
        }
    }
};

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    File(string filename_):
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

