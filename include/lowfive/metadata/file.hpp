#pragma once

namespace LowFive
{

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    File(std::string filename_):
        Object(ObjectType::File, filename_)
    {
        // debug
        fmt::print(stderr, "Creating metadata for {}\n", filename_);
    }

    // preorder depth-first traversal
    void print() const
    {
        fmt::print(stderr, "\nPrinting metadata tree for {}\n", name);
        Object::print(0);
        fmt::print(stderr, "\n");
    }
};

}
