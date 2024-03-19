#pragma once

namespace LowFive
{

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    using References = std::unordered_map<H5R_ref_t*, std::uintptr_t>;

    Hid             fcpl;                   // hdf5 id of file creation property list
    Hid             fapl;                   // hdf5 id of file access property list
    References      references;

    bool            copy_whole = false;
    bool            copy_of_remote = false;

    File(std::string filename_, Hid fcpl_, Hid fapl_):
        Object(ObjectType::File, filename_), fcpl(H5Pcopy(fcpl_.id)), fapl(H5Pcopy(fapl_.id))
    {}

    // preorder depth-first traversal
    void print() const
    {
        fmt::print(stderr, "\nPrinting metadata tree for {}\n", name);
        Object::print(0);
        fmt::print(stderr, "\n");
    }
};

}
