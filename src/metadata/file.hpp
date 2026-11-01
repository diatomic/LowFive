#pragma once
#include "dataspace.hpp"

namespace LowFive
{

// (root of) the tree of metadata for one HDF5 "file"
struct File: public Object
{
    struct PendingReference
    {
        H5R_type_t     type = H5R_BADTYPE;
        std::string    filename;
        std::uintptr_t token = 0;
        std::string    path;
        std::string    attribute;
        Dataspace      region;
        H5O_token_t    connector_token {};
        bool            resolved = false;
    };
    using References = std::unordered_map<H5R_ref_t*, PendingReference>;

    Hid             fcpl;                   // hdf5 id of file creation property list
    Hid             fapl;                   // hdf5 id of file access property list
    References      references;
    hid_t           reference_file_id = H5I_INVALID_HID;
    std::vector<Object*> unlinked;

    bool            copy_whole = false;
    bool            copy_of_remote = false;

    File(std::string filename_, Hid fcpl_, Hid fapl_):
        Object(ObjectType::File, filename_), fcpl(H5Pcopy(fcpl_.id), true), fapl(H5Pcopy(fapl_.id), true)
    {}

    File(const File& other):
        Object(ObjectType::File, other.name),
        fcpl(other.fcpl),
        fapl(other.fapl),
        references(other.references),
        copy_whole(other.copy_whole),
        copy_of_remote(other.copy_of_remote)
    {
        token = other.token;
    }

    ~File() override
    {
        for (auto* object : unlinked)
            delete object;
    }
    Object* find_token(std::uintptr_t token) override
    {
        if (auto* object = Object::find_token(token))
            return object;
        for (auto* object : unlinked)
            if (auto* found = object->find_token(token))
                return found;
        return nullptr;
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
