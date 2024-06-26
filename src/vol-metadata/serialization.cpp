#include "../vol-metadata-private.hpp"
#include "../metadata/remote.hpp"       // DM: I don't remember why remote.hpp wasn't included in metadata.hpp

void
LowFive::serialize(diy::MemoryBuffer& bb, Object* o, MetadataVOL& vol, bool include_data)
{
    diy::save(bb, o->token);
    diy::save(bb, o->type);
    diy::save(bb, o->name);
    diy::save(bb, o->children.size());

    auto log = get_logger();
    log->trace("Writing {} with {} children (include_data = {})", o->name, o->children.size(), include_data);

    if (o->type == ObjectType::File)
    {
        if (dynamic_cast<DummyFile*>(o) || dynamic_cast<RemoteFile*>(o))
        {
            diy::save(bb, false);
            return;
        }
        diy::save(bb, true);       // real_file = true

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
            diy::save(bb, d->strings);
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
            if (base_type.dtype_class == DatatypeClass::Reference)
                diy::save(bb, true);
            else
                diy::save(bb, false);
            for (size_t i = 0; i < a->space.size(); ++i)
            {
                auto len = data_hvl[i].len;
                diy::save(bb, len);
                if (base_type.dtype_class == DatatypeClass::Reference)
                {
                    for (decltype(len) j = 0; j < len; ++j)
                    {
                        auto* ref = (H5R_ref_t*) data_hvl[i].p + j;

                        hid_t obj_id = H5Ropen_object(ref, H5P_DEFAULT, H5P_DEFAULT);
                        H5O_info2_t oinfo;
                        H5Oget_info(obj_id, &oinfo, H5O_INFO_ALL);

                        std::string name(1000, '\0');
                        H5Rget_obj_name(ref, H5P_DEFAULT, name.data(), name.size());

                        std::uintptr_t* token = (std::uintptr_t*) oinfo.token.__data;
                        diy::save(bb, *token);
                        log->info("Saving reference token: {} -> {}", *token, name);

                        H5Oclose(obj_id);
                    }
                } else
                {
                    size_t count = len * base_type.dtype_size;
                    diy::save(bb, (char*) data_hvl[i].p, count);
                }
            }
        } else
            diy::save(bb, a->data.get(), a->mem_type.dtype_size * a->space.size());

        // TODO
        // add support for serializing compound types
    }
    else if (o->type == ObjectType::HardLink)
        diy::save(bb, static_cast<HardLink*>(o)->target->fullname().second);
    else if (o->type == ObjectType::SoftLink)
        diy::save(bb, static_cast<SoftLink*>(o)->target);
    else if (o->type == ObjectType::CommittedDatatype)
        diy::save(bb, static_cast<CommittedDatatype*>(o)->data);

    for (Object* child : o->children)
        serialize(bb, child, vol, include_data);
}

namespace LowFive
{
using HardLinks = std::unordered_map<HardLink*, std::string>;
using References = File::References;
Object* deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, References& references, bool include_data);
}

// top-level call
LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb, MetadataVOL& vol)
{
    HardLinks hard_links;
    References references;
    auto* o = deserialize(bb, hard_links, references, false);

    // link the hard links
    for (auto& x : hard_links)
        x.first->target = o->search(x.second).exact();

    File* f = dynamic_cast<File*>(o);
    if (f)
        f->references = std::move(references);

    return o;
}

LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, References& references, bool include_data)
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
        bool real_file;
        diy::load(bb, real_file);
        if (!real_file)
            return new DummyFile(name);     // this is potentially problematic for RemoteFile, but we shouldn't be broadcasting those in the first place

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
            diy::load(bb, d->strings);

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

        if (a->mem_type.is_var_length_string())
            diy::load(bb, a->strings);
        else if (a->mem_type.dtype_class == DatatypeClass::VarLen)
        {
            size_t nbytes = a->space.size() * sizeof(hvl_t);
            a->data = std::unique_ptr<char>(new char[nbytes]);

            hvl_t* data_hvl = (hvl_t*) a->data.get();
            auto base_type = Datatype(H5Tget_super(a->mem_type.id));

            bool is_reference;
            diy::load(bb, is_reference);

            for (size_t i = 0; i < a->space.size(); ++i)
            {
                using LenType = decltype(data_hvl[i].len);
                LenType len;
                diy::load(bb, len);
                data_hvl[i].len = len;
                size_t count = len * base_type.dtype_size;
                data_hvl[i].p = new char[count];

                if (is_reference)
                {
                    for (LenType j = 0; j < len; ++j)
                    {
                        std::uintptr_t token;
                        diy::load(bb, token);
                        log->info("Loaded a reference token {}", token);
                        references.emplace((H5R_ref_t*) data_hvl[i].p + j, token);
                    }
                } else
                {
                    diy::load(bb, (char*) data_hvl[i].p, count);
                }
            }
        }
        else
        {
            size_t nbytes = a->mem_type.dtype_size * a->space.size();
            a->data = std::unique_ptr<char>(new char[nbytes]);
            diy::load(bb, a->data.get(), nbytes);
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
        MetadataError("unhandled case in deserialization");

    o->token = token;

    for (size_t i = 0; i < n_children; ++i)
        o->add_child(deserialize(bb, hard_links, references, include_data));

    return o;
}
