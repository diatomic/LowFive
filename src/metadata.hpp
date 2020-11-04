#pragma once

using namespace std;

#define MAX_DIM 32                                      // this is HDF5's limit

enum ObjectType
{
    Group,
    Dataset,
    NamedDatatype
};

enum AtomicDtype
{
    Time,
    Bitfield,
    String,
    Opaque,
    Integer,
    Float
};

enum CompositeDtype
{
    Array,
    Enum,
    VarLength,
    Compound
};

struct Dataspace
{
    int                             dim;
    vector<size_t>                  min;
    vector<size_t>                  max;
};

struct Datatype
{
    bool                            atomic;
    AtomicDtype                     a_type;
    CompositeDtype                  c_type;
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
    vector<Dataspace>               m_dataspaces;       // memory dataspaces
    vector<Dataspace>               f_dataspaces;       // file dataspaces
    Datatype                        datatype;
    map<string, Attribute>          attributes;
    map<string, Attribute>          properties;

    Object(ObjectType type_, string name_) :
        type(type_),
        name(name_)                                     {}
};

// a node of a generic tree, not necessarily HDF5, templated on the object being stored
// in this use case, T is Object
template<class T>
struct TreeNode
{
    TreeNode<T>*                    parent;
    vector<TreeNode<T>*>            children;
    T*                              node_data;          // pointer to the object being stored (eg., Object)

    TreeNode(TreeNode<T>* parent_, T* node_data_) :
        parent(parent_), node_data(node_data_) {}
};

// the (root of) tree of metadata for one HDF5 "file"
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

    TreeNode<Object>* add_node(TreeNode<Object>* parent, Object* node_data)
    {
        TreeNode<Object>* tree_node = new TreeNode<Object>(parent, node_data);
        if (parent == NULL)
            children.push_back(tree_node);
        else
            parent->children.push_back(tree_node);
        return tree_node;

        // debug
        fmt::print(stderr, "Added metadata node: name {} type {}\n", node_data->name, node_data->type);
    }
};

