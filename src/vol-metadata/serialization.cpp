#include "../vol-metadata-private.hpp"

void
LowFive::serialize(diy::MemoryBuffer& bb, Object* o, bool include_data)
{
    diy::save(bb, o->token);
    diy::save(bb, o->type);
    diy::save(bb, o->name);
    diy::save(bb, o->children.size());

    auto log = get_logger();
    log->trace("Writing {} with {} children (include_data = {})", o->name, o->children.size(), include_data);

    if (o->type == ObjectType::File)
    {
        include_data = static_cast<File*>(o)->copy_whole;
        diy::save(bb, include_data);
    }
    if (o->type == ObjectType::Dataset)
    {
        auto* d = static_cast<Dataset*>(o);
        diy::save(bb, d->is_passthru);
        diy::save(bb, d->is_memory);
        diy::save(bb, d->type);
        diy::save(bb, d->space);
        diy::save(bb, d->ownership);

        if (include_data)
        {
            diy::save(bb, d->data.size());

            // serialize triplets
            for (auto& dt : d->data)
            {
                diy::save(bb, dt.type);
                diy::save(bb, dt.memory);
                diy::save(bb, dt.file);
                size_t nbytes   = dt.memory.size() * dt.type.dtype_size;
                bb.save_binary_blob(static_cast<const char*>(dt.data), nbytes);
                log->trace("position = {}, size = {}", bb.position, bb.size());
            }
        }
    }
    else if (o->type == ObjectType::Attribute)
    {
        auto* a = static_cast<Attribute*>(o);
        diy::save(bb, a->type);
        diy::save(bb, a->space);
        diy::save(bb, a->mem_type);
        if (a->mem_type.is_var_length_string())
            diy::save(bb, a->strings);
        else if (a->mem_type.dtype_class == DatatypeClass::VarLen)
        {
            hvl_t* data_hvl = (hvl_t*) a->data.get();
            auto base_type = Datatype(H5Tget_super(a->mem_type.id));
            for (size_t i = 0; i < a->space.size(); ++i)
            {
                auto len = data_hvl[i].len;
                size_t count = len * base_type.dtype_size;
                diy::save(bb, len);
                diy::save(bb, (char*) data_hvl[i].p, count);
            }
        } else
            diy::save(bb, a->data.get(), a->mem_type.dtype_size);
    }
    else if (o->type == ObjectType::HardLink)
        diy::save(bb, static_cast<HardLink*>(o)->target->fullname().second);
    else if (o->type == ObjectType::SoftLink)
        diy::save(bb, static_cast<SoftLink*>(o)->target);
    else if (o->type == ObjectType::CommittedDatatype)
        diy::save(bb, static_cast<CommittedDatatype*>(o)->data);

    for (Object* child : o->children)
        serialize(bb, child, include_data);
}

namespace LowFive
{
using HardLinks = std::unordered_map<HardLink*, std::string>;
Object* deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, bool include_data);
}

// top-level call
LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb)
{
    HardLinks hard_links;
    auto* o = deserialize(bb, hard_links, false);

    // link the hard links
    for (auto& x : hard_links)
        x.first->target = o->search(x.second).exact();

    return o;
}

LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, bool include_data)
{
    std::uintptr_t token;
    ObjectType type;
    std::string name;
    size_t n_children;

    diy::load(bb, token);
    diy::load(bb, type);
    diy::load(bb, name);
    diy::load(bb, n_children);

    auto log = get_logger();
    log->trace("Reading {} with {} children (include_data = {})", name, n_children, include_data);

    // figure out the type
    Object* o;
    if (type == ObjectType::File)
    {
        diy::load(bb, include_data);
        File* f = new File(name, H5Pcreate(H5P_FILE_CREATE), H5Pcreate(H5P_FILE_ACCESS));
        f->copy_whole = include_data;
        f->copy_of_remote = include_data;
        o = f;
    } else if (type == ObjectType::Group)
        o = new Group(name, H5P_GROUP_CREATE_DEFAULT);
    else if (type == ObjectType::Dataset)
    {
        bool is_passthru, is_memory;
        Datatype dt;
        Dataspace s;
        Dataset::Ownership own;

        diy::load(bb, is_passthru);
        diy::load(bb, is_memory);
        diy::load(bb, dt);
        diy::load(bb, s);
        diy::load(bb, own);

        auto* d = new Dataset(name, dt.id, s.id, own, H5Pcreate(H5P_DATASET_CREATE), H5Pcreate(H5P_DATASET_ACCESS), is_passthru, is_memory);
        o = d;

        // load triplets
        if (include_data)
        {
            // deserialize triplets
            size_t ntriplets;
            diy::load(bb, ntriplets);
            log->trace("Reading {} triplets from buffer with {} blobs", ntriplets, bb.nblobs());
            for (size_t i = 0; i < ntriplets; ++i)
            {
                log->trace("position = {}, size = {}", bb.position, bb.size());
                Datatype type; diy::load(bb, type);
                Dataspace memory; diy::load(bb, memory);
                Dataspace file; diy::load(bb, file);

                auto blob = bb.load_binary_blob();
                char* p = (char*) blob.pointer.get();
                assert(blob.size == memory.size() * type.dtype_size);
                d->data.emplace_back(Dataset::DataTriple { type, memory, file, p, std::unique_ptr<char[]>(p) });

                log->trace("Read binary blob of size {}", blob.size);
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

        {
        }
        if (a->mem_type.is_var_length_string())
            diy::load(bb, a->strings);
        else if (a->mem_type.dtype_class == DatatypeClass::VarLen)
        {
            size_t nbytes = a->space.size() * sizeof(hvl_t);
            a->data = std::unique_ptr<char>(new char[nbytes]);

            hvl_t* data_hvl = (hvl_t*) a->data.get();
            auto base_type = Datatype(H5Tget_super(a->mem_type.id));

            for (size_t i = 0; i < a->space.size(); ++i)
            {
                decltype(data_hvl[i].len) len;
                diy::load(bb, len);
                data_hvl[i].len = len;
                size_t count = len * base_type.dtype_size;
                data_hvl[i].p = new char[count];
                diy::load(bb, (char*) data_hvl[i].p, count);
            }
        }
        else
        {
            a->data = std::unique_ptr<char>(new char[a->mem_type.dtype_size]);
            diy::load(bb, a->data.get(), a->mem_type.dtype_size);
        }

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
    }
    else if (type == ObjectType::CommittedDatatype)
    {
        auto* dt = new CommittedDatatype(name, 0);
        diy::load(bb, dt->data);
        o = dt;
    } else
        throw MetadataError("unhandled case in deserialization");

    o->token = token;

    for (size_t i = 0; i < n_children; ++i)
        o->add_child(deserialize(bb, hard_links, include_data));

    return o;
}
