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

struct H5Object
{
    ObjectType                      type;
    string                          name;
    Dataspace                       dataspace;
    Datatype                        datatype;
    map<string, Attribute>          attributes;
    map<string, Attribute>          properties;

    H5Object(ObjectType type_, string name_) :
        type(type_),
        name(name_)                                     {}
};

// a node of a generic tree, not necessarily HDF5, templated on the object being stored
// in this use case, T is H5Object
template<class T>
struct TreeNode
{
    size_t                          id;                 // unique id
    TreeNode<T>*                    parent;
    vector<TreeNode<T>*>            children;
    T*                              node_data;          // pointer to the object being stored (eg., H5Object)

    TreeNode(size_t id_, TreeNode<T>* parent_, T* node_data_) :
        id(id_), parent(parent_), node_data(node_data_) {}
};

// the (root of) tree of metadata for one HDF5 "file"
struct FileMetadata
{
    string                              filename;
    vector<TreeNode<H5Object>*>         children;
    map<size_t, TreeNode<H5Object>*>    node_map;       // index into nodes keyed by id

    FileMetadata(string filename_) :
        filename(filename_)                             { fmt::print(stderr, "Creating metadata for {}\n", filename); }

    void add_node(size_t id, TreeNode<H5Object>* parent, H5Object* node_data)
    {
        TreeNode<H5Object>* tree_node = new TreeNode<H5Object>(id, parent, node_data);
        if (parent == NULL)
            children.push_back(tree_node);
        else
            parent->children.push_back(tree_node);
        node_map.insert(pair<size_t, TreeNode<H5Object>*>(id, tree_node));

        fmt::print(stderr, "Added metadata node: name {} type {} node_map size {}\n", node_data->name, node_data->type, node_map.size());
    }

    TreeNode<H5Object>* node(size_t id)                 { return node_map.at(id); }
};

