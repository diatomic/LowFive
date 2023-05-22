#include "../vol-metadata-private.hpp"

void
LowFive::serialize(diy::BinaryBuffer& bb, Object* o, bool include_data)
{
    diy::save(bb, o->type);
    diy::save(bb, o->name);
    diy::save(bb, o->children.size());

    if (o->type == ObjectType::File)
    {
        include_data = static_cast<File*>(o)->copy_whole;
        diy::save(bb, include_data);
    }
    if (o->type == ObjectType::Dataset)
    {
        auto* d = static_cast<Dataset*>(o);
        diy::save(bb, d->type);
        diy::save(bb, d->space);
        diy::save(bb, d->ownership);

        if (include_data)
        {
            // serialize triplets
            diy::save(bb, d->data.size());
            for (auto& dt : d->data)
            {
                diy::save(bb, dt.type);
                diy::save(bb, dt.memory);
                diy::save(bb, dt.file);
                size_t nbytes   = dt.memory.size() * dt.type.dtype_size;
                bb.save_binary(static_cast<const char*>(dt.data), nbytes);
            }
        }
    }
    else if (o->type == ObjectType::Attribute)
    {
        auto* a = static_cast<Attribute*>(o);
        diy::save(bb, a->type);
        diy::save(bb, a->space);
        diy::save(bb, a->mem_type);
        if (!a->mem_type.is_var_length_string())
            diy::save(bb, a->data.get(), a->mem_type.dtype_size);
        else
            diy::save(bb, a->strings);
    }
    else if (o->type == ObjectType::HardLink)
        diy::save(bb, static_cast<HardLink*>(o)->target->fullname().second);
    else if (o->type == ObjectType::SoftLink)
        diy::save(bb, static_cast<SoftLink*>(o)->target);

    for (Object* child : o->children)
        serialize(bb, child, include_data);
}

namespace LowFive
{
using HardLinks = std::unordered_map<HardLink*, std::string>;
Object* deserialize(diy::BinaryBuffer& bb, HardLinks& hard_links, bool include_data);
}

// top-level call
LowFive::Object*
LowFive::deserialize(diy::BinaryBuffer& bb)
{
    HardLinks hard_links;
    auto* o = deserialize(bb, hard_links, false);

    // link the hard links
    for (auto& x : hard_links)
        x.first->target = o->search(x.second).exact();

    return o;
}

LowFive::Object*
LowFive::deserialize(diy::BinaryBuffer& bb, HardLinks& hard_links, bool include_data)
{
    ObjectType type;
    std::string name;
    size_t n_children;

    diy::load(bb, type);
    diy::load(bb, name);
    diy::load(bb, n_children);

    // figure out the type
    Object* o;
    if (type == ObjectType::File)
    {
        diy::load(bb, include_data);
        File* f = new File(name, H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
        f->copy_whole = include_data;
        o = f;
    } else if (type == ObjectType::Group)
        o = new Group(name, H5P_GROUP_CREATE_DEFAULT);
    else if (type == ObjectType::Dataset)
    {
        Datatype dt;
        Dataspace s;
        Dataset::Ownership own;

        diy::load(bb, dt);
        diy::load(bb, s);
        diy::load(bb, own);

        auto* d = new Dataset(name, dt.id, s.id, own, H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT);
        o = d;

        // load triplets
        if (include_data)
        {
            // deserialize triplets
            size_t ntriplets;
            diy::load(bb, ntriplets);
            for (size_t i = 0; i < ntriplets; ++i)
            {
                Datatype type; diy::load(bb, type);
                Dataspace memory; diy::load(bb, memory);
                Dataspace file; diy::load(bb, file);
                size_t nbytes   = memory.size() * type.dtype_size;
                char* p         = new char[nbytes];
                bb.load_binary(p, nbytes);
                d->data.emplace_back(Dataset::DataTriple { type, memory, file, p, std::unique_ptr<char[]>(p) });
            }
        }
    }
    else if (type == ObjectType::Attribute)
    {
        Datatype dt;
        Dataspace s;

        diy::load(bb, dt);
        diy::load(bb, s);

        auto* a = new Attribute(name, dt.id, s.id);
        diy::load(bb, a->mem_type);
        if (!a->mem_type.is_var_length_string())
        {
            a->data = std::unique_ptr<char>(new char[a->mem_type.dtype_size]);
            diy::load(bb, a->data.get(), a->mem_type.dtype_size);
        } else
            diy::load(bb, a->strings);

        o = a;
    }
    else if (type == ObjectType::NamedDtype)
        o = new NamedDtype(name);
    else if (type == ObjectType::HardLink)
    {
        std::string target;
        diy::load(bb, target);
        auto* hl = new HardLink(name, nullptr);
        hard_links.emplace(hl, target);
        o = hl;
    }
    else if (type == ObjectType::SoftLink)
    {
        std::string target;
        diy::load(bb, target);
        o = new SoftLink(name, target);
    } else
        MetadataError("unhandled case in deserialization");

    for (size_t i = 0; i < n_children; ++i)
        o->add_child(deserialize(bb, hard_links, include_data));

    return o;
}
