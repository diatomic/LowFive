#include <lowfive/vol-metadata.hpp>
#include "metadata.hpp"
#include "vol-metadata-private.hpp"

void
LowFive::MetadataVOL::
wrap(LowFive::Object* mdata_obj, void* h5_obj)
{
    mdata_obj->h5_obj = h5_obj;
    our_objects.insert(mdata_obj);
}

void*
LowFive::MetadataVOL::
unwrap(LowFive::Object* p)
{
    return p->h5_obj;
}

void
LowFive::MetadataVOL::
drop(void* p)
{
    auto log = get_logger();
//    log->trace("MetadataVOL drop, p = {}", fmt::ptr(p));
//    our_objects.erase(p);
//    Object* op = static_cast<Object*>(p);
//    delete op;
}

LowFive::MetadataVOL::
~MetadataVOL()
{
    auto log = get_logger();
    log->trace("MetadataVOL dtor, calling clear_files");
    clear_files();
    log->trace("MetadataVOL dtor, clear files done");
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
    for (auto& x : files) {
        delete x.second;
    }
    files.clear();
}

LowFive::MetadataVOL& LowFive::MetadataVOL::
create_MetadataVOL()
{
    auto log = get_logger();
    log->trace("Enter get_metadata_vol");

    if (!info->vol)
    {
        log->trace("In get_metadata_vol: info->vol is NULL, creating new MetadataVOL");
        info->vol = new MetadataVOL();
    } else
    {
        log->warn("In get_metadata_vol: info->vol is not NULL, return existing object");
    }

    return *dynamic_cast<MetadataVOL*>(info->vol);
}
