#include <lowfive/vol-metadata.hpp>
#include "metadata.hpp"
#include "vol-metadata-private.hpp"

LowFive::ObjectPointers*
LowFive::MetadataVOL::
wrap(void* p)
{
    ObjectPointers* op = new ObjectPointers;
    op->h5_obj = p;

    our_objects.insert(op);

    return op;
}

void*
LowFive::MetadataVOL::
unwrap(void* p)
{
    ObjectPointers* op = static_cast<ObjectPointers*>(p);
    return op->h5_obj;
}

void
LowFive::MetadataVOL::
drop(void* p)
{
    our_objects.erase(p);
    ObjectPointers* op = static_cast<ObjectPointers*>(p);
    delete op;
}

LowFive::MetadataVOL::
~MetadataVOL()
{
    clear_files();
}

void
LowFive::MetadataVOL::
drop(std::string filename)
{
    auto it = files.find(filename);
    delete it->second;
    files.erase(it);
}

void
LowFive::MetadataVOL::
print_files()
{
    for (auto& f : files)
    {
        fmt::print("--------------------------------------------------------------------------------\n");
        fmt::print("File {}\n", f.first);
        f.second->print(0);
        fmt::print("--------------------------------------------------------------------------------\n");
    }
}

// locate an object in the metadata of one file by its full path, which uniquely identifies one object
LowFive::Object*
LowFive::MetadataVOL::
locate(std::string filename, std::string full_path) const
{
    auto it = files.find(filename);
    if (it == files.end())
        return NULL;

    return it->second->search(full_path).exact();
}

    
void
LowFive::MetadataVOL::
clear_files()
{
    for (auto& x : files)
        delete x.second;
    files.clear();
}
