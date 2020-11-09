#pragma once

using namespace std;

#define MAX_DIM 32                                      // this is HDF5's limit

enum class ObjectType
{
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
    ObjectType                      type;
    string                          name;
    map<string, Attribute>          attributes;
    map<string, Attribute>          properties;

    Object(ObjectType type_, string name_) :
        type(type_), name(name_)                        {}

    virtual void print()
    {
        fmt::print(stderr, "object type = {} name = {}\n", type, name);
        // TODO: print attributes and properties
    }
};

struct Group : public Object
{
    vector<Object*>                 objects;

    Group(string name) :
        Object(ObjectType::Group, name)                 {}

    void print()
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

    void print()
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

    void print()
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

// a node of the HDF5 "file" tree, templated on the object being stored
template<class T>
struct TreeNode
{
    TreeNode<T>*                    parent;
    vector<TreeNode<T>*>            children;
    T*                              object;             // pointer to the object being stored

    TreeNode(TreeNode<T>* parent_, T* object_) :
        parent(parent_), object(object_)                {}

    // preorder depth-first traversal
    void print()
    {
        object->print();
        for (auto i = 0; i < children.size(); i++)
            children[i]->print();
    }
};

// (root of) the tree of metadata for one HDF5 "file"
struct FileMetadata
{
    string                          filename;
    vector<TreeNode<Object>*>       children;

    FileMetadata(string filename_) :
        filename(filename_)
    {
        // debug
        fmt::print(stderr, "Creating metadata for {}\n", filename);
    }

    TreeNode<Object>* add_node(TreeNode<Object>* parent, Object* object)
    {
        TreeNode<Object>* tree_node = new TreeNode<Object>(parent, object);
        if (parent == NULL)
            children.push_back(tree_node);
        else
            parent->children.push_back(tree_node);
        return tree_node;

        // debug
        fmt::print(stderr, "Added metadata node: name {} type {}\n", object->name, object->type);
    }

    // preorder depth-first traversal
    void print_tree()
    {
        fmt::print(stderr, "\nPrinting metadata tree for {}\n", filename);
        for (auto i = 0; i < children.size(); i++)
            children[i]->print();
        fmt::print(stderr, "\n");
    }
};

