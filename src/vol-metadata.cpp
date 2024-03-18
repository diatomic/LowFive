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

void
LowFive::MetadataVOL::
resolve_references(Object* o)
{
    File* f = dynamic_cast<File*>(o->find_root());
    assert(f);

    if (f->references.size() == 0)
        return;

    auto log = get_logger();
    ObjectPointers* fop = wrap(nullptr);
    fop->mdata_obj = f;

    hid_t fid = H5VLwrap_register(fop, H5I_FILE);

    for (auto& x : f->references)
    {
        Object* o = f->find_token(x.second);
        log_assert(o, "Must be able to find object by token");
        auto path = o->fullname().second;
        auto ret = H5Rcreate_object(fid, path.c_str(), H5P_DEFAULT, x.first);
        log->debug("Created reference to {} using token {}", path, x.second);
    }

    f->references.clear();

    //H5Idec_ref(fid);
}
