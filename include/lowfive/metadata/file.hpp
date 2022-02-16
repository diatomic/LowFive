#pragma once

namespace LowFive
{

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    Hid             fcpl;                   // hdf5 id of file creation property list
    Hid             fapl;                   // hdf5 id of file access property list

    File(std::string filename_, Hid fcpl_, Hid fapl_):
        Object(ObjectType::File, filename_), fcpl(fcpl_), fapl(fapl_)
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
