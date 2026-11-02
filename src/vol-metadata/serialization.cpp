#include "../vol-metadata-private.hpp"
#include <unordered_set>


namespace
{
// Walk the datatype rather than transmitting process-local pointers in compound,
// array, variable-length, or modern reference values.
template<bool Load>
void serialized_values(diy::MemoryBuffer& bb, const LowFive::Datatype& type, size_t count,
                       char* data, LowFive::File::References* references = nullptr)
{
    const auto cls = H5Tget_class(type.id);
    if (cls == H5T_COMPOUND)
    {
        for (int member = 0; member < H5Tget_nmembers(type.id); ++member)
        {
            LowFive::Datatype member_type(H5Tget_member_type(type.id, member), true);
            const size_t offset = H5Tget_member_offset(type.id, member);
            for (size_t i = 0; i < count; ++i)
                serialized_values<Load>(bb, member_type, 1, data + i * type.dtype_size + offset, references);
        }
    }
    else if (cls == H5T_ARRAY)
    {
        LowFive::Datatype base(H5Tget_super(type.id), true);
        serialized_values<Load>(bb, base, count * type.dtype_size / base.dtype_size, data, references);
    }
    else if (cls == H5T_VLEN)
    {
        LowFive::Datatype base(H5Tget_super(type.id), true);
        auto* arrays = reinterpret_cast<hvl_t*>(data);
        for (size_t i = 0; i < count; ++i)
        {
            if constexpr (Load)
            {
                diy::load(bb, arrays[i].len);
                arrays[i].p = arrays[i].len ? H5allocate_memory(arrays[i].len * base.dtype_size, true) : nullptr;
            }
            else
                diy::save(bb, arrays[i].len);
            serialized_values<Load>(bb, base, arrays[i].len, static_cast<char*>(arrays[i].p), references);
        }
    }
    else if (type.is_var_length_string())
    {
        auto** strings = reinterpret_cast<char**>(data);
        for (size_t i = 0; i < count; ++i)
        {
            bool present;
            if constexpr (Load)
                diy::load(bb, present);
            else
            {
                present = strings[i] != nullptr;
                diy::save(bb, present);
            }
            if (!present) continue;
            std::string value;
            if constexpr (Load)
            {
                diy::load(bb, value);
                strings[i] = static_cast<char*>(H5allocate_memory(value.size() + 1, false));
                std::memcpy(strings[i], value.c_str(), value.size() + 1);
            }
            else
            {
                value = strings[i];
                diy::save(bb, value);
            }
        }
    }
    else if (type.is_modern_reference())
    {
        auto* refs = reinterpret_cast<H5R_ref_t*>(data);
        for (size_t i = 0; i < count; ++i)
        {
            bool present;
            if constexpr (Load)
                diy::load(bb, present);
            else
            {
                const H5R_ref_t empty {};
                present = (references &&
                           references->find(refs + i) != references->end()) ||
                          (std::memcmp(refs + i, &empty, sizeof(empty)) != 0 &&
                           H5Rget_type(refs + i) != H5R_BADTYPE);
                diy::save(bb, present);
            }
            if (!present) continue;
            LowFive::File::PendingReference state;
            if constexpr (Load)
            {
                diy::load(bb, state.type);
                diy::load(bb, state.filename);
                diy::load(bb, state.path);
                diy::load(bb, state.token);
                diy::load(bb, state.connector_token.__data,
                          H5O_MAX_TOKEN_SIZE);
                if (state.type == H5R_ATTR)
                    diy::load(bb, state.attribute);
                else if (state.type == H5R_DATASET_REGION1 ||
                         state.type == H5R_DATASET_REGION2)
                    diy::load(bb, state.region);
                references->emplace(refs + i, std::move(state));
            }
            else
            {
                auto pending = references ? references->find(refs + i) :
                                            LowFive::File::References::iterator {};
                state = references && pending != references->end() ?
                    pending->second : LowFive::describe_reference(refs[i]);
                diy::save(bb, state.type);
                diy::save(bb, state.filename);
                diy::save(bb, state.path);
                diy::save(bb, state.token);
                diy::save(bb, state.connector_token.__data,
                          H5O_MAX_TOKEN_SIZE);
                if (state.type == H5R_ATTR)
                    diy::save(bb, state.attribute);
                else if (state.type == H5R_DATASET_REGION1 ||
                         state.type == H5R_DATASET_REGION2)
                    diy::save(bb, state.region);
            }
        }
    }
    else if (count)
    {
        if constexpr (Load)
            diy::load(bb, data, count * type.dtype_size);
        else
            diy::save(bb, data, count * type.dtype_size);
    }
}

template<bool Load>
void serialized_fill(diy::MemoryBuffer& bb, LowFive::Dataset& dataset,
                     LowFive::File::References* references)
{
    H5D_fill_value_t status = H5D_FILL_VALUE_DEFAULT;
    if constexpr (Load)
        diy::load(bb, status);
    else
    {
        if (H5Pfill_value_defined(dataset.dcpl.id, &status) < 0)
            throw LowFive::MetadataError("Could not query serialized dataset fill value");
        diy::save(bb, status);
    }
    if (status == H5D_FILL_VALUE_DEFAULT)
        return;
    if (status == H5D_FILL_VALUE_UNDEFINED)
    {
        if constexpr (Load)
            if (H5Pset_fill_value(dataset.dcpl.id, dataset.type.id, nullptr) < 0)
                throw LowFive::MetadataError("Could not restore undefined dataset fill value");
        return;
    }

    const auto& type = dataset.type;
    if constexpr (Load)
    {
        dataset.fill_data.reset(new char[type.dtype_size]());
        serialized_values<true>(
            bb, type, 1, dataset.fill_data.get(), references);
        if (H5Pset_fill_value(dataset.dcpl.id, type.id, dataset.fill_data.get()) < 0)
            throw LowFive::MetadataError("Could not restore serialized dataset fill value");
    }
    else if (dataset.fill_data)
        serialized_values<false>(
            bb, type, 1, dataset.fill_data.get(), references);
    else
    {
        std::vector<char> value(type.dtype_size, 0);
        try
        {
            if (H5Pget_fill_value(dataset.dcpl.id, type.id, value.data()) < 0)
                throw LowFive::MetadataError("Could not read serialized dataset fill value");
            serialized_values<false>(bb, type, 1, value.data(), references);
        }
        catch (...)
        {
            type.destroy_values(1, value.data(), false);
            throw;
        }
        type.destroy_values(1, value.data(), false);
    }
}

std::vector<LowFive::Object*> reachable_unlinked(LowFive::File& file)
{
    std::unordered_set<LowFive::Object*> detached(file.unlinked.begin(),
                                                  file.unlinked.end());
    std::unordered_set<LowFive::Object*> marked;
    std::vector<LowFive::Object*> pending;
    auto mark = [&](LowFive::Object* target)
    {
        if (!target || target->find_root() != &file)
            return;
        while (target->parent)
            target = target->parent;
        if (detached.count(target) && marked.insert(target).second)
            pending.push_back(target);
    };
    auto scan = [&](LowFive::Object* object, auto&& scan_ref) -> void
    {
        if (auto* link = dynamic_cast<LowFive::HardLink*>(object))
            mark(link->target);
        for (auto* child : object->children)
            scan_ref(child, scan_ref);
    };
    scan(&file, scan);
    for (size_t i = 0; i < pending.size(); ++i)
        scan(pending[i], scan);

    std::vector<LowFive::Object*> result;
    result.reserve(marked.size());
    for (auto* object : file.unlinked)
        if (marked.count(object))
            result.push_back(object);
    return result;
}

}

void
LowFive::serialize_values(diy::MemoryBuffer& bb, const Datatype& type,
                          size_t count, const void* values,
                          File::References* references)
{
    serialized_values<false>(
        bb, type, count, const_cast<char*>(static_cast<const char*>(values)),
        references);
}

void
LowFive::deserialize_values(diy::MemoryBuffer& bb, const Datatype& type,
                            size_t count, void* values,
                            File::References* references)
{
    serialized_values<true>(
        bb, type, count, static_cast<char*>(values), references);
}

void
LowFive::serialize(diy::MemoryBuffer& bb, Object* o, MetadataVOL& vol, bool include_data)
{
    std::vector<Object*> unlinked;
    diy::save(bb, o->token);
    diy::save(bb, o->type);
    diy::save(bb, o->name);
    diy::save(bb, o->children.size());

    auto log = get_logger();
    log->trace("Writing {} with {} children (include_data = {})", o->name, o->children.size(), include_data);

    auto* file = dynamic_cast<File*>(o->find_root());
    auto* references = file ? &file->references : nullptr;

    if (o->type == ObjectType::File)
    {
        if (dynamic_cast<DummyFile*>(o) ||
            static_cast<File*>(o)->remote)
        {
            diy::save(bb, false);
            return;
        }
        diy::save(bb, true);       // real_file = true

        include_data = static_cast<File*>(o)->copy_whole;
        diy::save(bb, include_data);
        unlinked = reachable_unlinked(*static_cast<File*>(o));
        diy::save(bb, unlinked.size());
    }
    if (o->type == ObjectType::Dataset)
    {
        auto* d = static_cast<Dataset*>(o);
        diy::save(bb, d->is_passthru);
        diy::save(bb, d->is_memory);
        diy::save(bb, d->type);
        diy::save(bb, d->space);
        diy::save(bb, d->ownership);
        serialized_fill<false>(bb, *d, references);

        if (include_data)
        {
            diy::save(bb, d->strings);
            diy::save(bb, d->data.size());

            for (auto& dt : d->data)
            {
                diy::save(bb, dt.type);
                diy::save(bb, dt.memory);
                diy::save(bb, dt.file);
                Dataspace::iterate(dt.memory, 1, [&](size_t offset, size_t count)
                {
                    auto* values = const_cast<char*>(static_cast<const char*>(dt.data)) +
                                   offset * dt.type.dtype_size;
                    if (dt.type.is_var_length_string())
                        diy::save(bb, values, count * dt.type.dtype_size);
                    else
                        serialized_values<false>(bb, dt.type, count, values,
                                                 references);
                });
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
        const bool has_data = static_cast<bool>(a->data);
        diy::save(bb, has_data);
        if (has_data)
            serialized_values<false>(bb, a->mem_type, a->space.size(),
                                     a->data.get(), references);
    }
    else if (o->type == ObjectType::HardLink)
    {
        auto* target = static_cast<HardLink*>(o)->target;
        diy::save(bb, target ? target->token : std::uintptr_t {});
    }
    else if (o->type == ObjectType::SoftLink)
        diy::save(bb, static_cast<SoftLink*>(o)->target);
    else if (o->type == ObjectType::CommittedDatatype)
        diy::save(bb, static_cast<CommittedDatatype*>(o)->data);

    for (Object* child : o->children)
        serialize(bb, child, vol, include_data);
    for (Object* object : unlinked)
        serialize(bb, object, vol, include_data);
}

namespace LowFive
{
using HardLinks = std::unordered_map<HardLink*, std::uintptr_t>;
using Objects = std::unordered_map<std::uintptr_t, Object*>;
using References = File::References;
Object* deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, Objects& objects,
                    References& references, bool include_data);
}

// top-level call
LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb, MetadataVOL& vol)
{
    HardLinks hard_links;
    Objects objects;
    References references;
    std::unique_ptr<Object> result(
        deserialize(bb, hard_links, objects, references, false));

    for (auto& link : hard_links)
    {
        if (!link.second)
            link.first->target = nullptr;
        else
        {
            auto target = objects.find(link.second);
            if (target == objects.end())
                throw MetadataError("Hard-link target was not serialized");
            link.first->target = target->second;
        }
    }

    if (auto* file = dynamic_cast<File*>(result.get()))
        file->references = std::move(references);

    return result.release();
}

LowFive::Object*
LowFive::deserialize(diy::MemoryBuffer& bb, HardLinks& hard_links, Objects& objects,
                     References& references, bool include_data)
{
    std::uintptr_t token;
    ObjectType type;
    std::string name;
    size_t n_children;

    size_t n_unlinked = 0;
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
        diy::load(bb, n_unlinked);
        Hid fcpl(H5Pcreate(H5P_FILE_CREATE), true);
        Hid fapl(H5Pcreate(H5P_FILE_ACCESS), true);
        File* f = new File(name, fcpl.id, fapl.id);
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

        auto dataset = std::make_unique<Dataset>(
            name, dt.id, s.id, own, Hid(H5Pcreate(H5P_DATASET_CREATE), true),
            Hid(H5Pcreate(H5P_DATASET_ACCESS), true), is_passthru, is_memory);
        serialized_fill<true>(bb, *dataset, &references);
        auto* d = dataset.release();
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

                const size_t count = memory.size();
                auto values = std::unique_ptr<char[]>(
                    new char[count * type.dtype_size]());
                Dataspace::iterate(memory, 1, [&](size_t offset, size_t selected)
                {
                    char* destination = values.get() + offset * type.dtype_size;
                    if (type.is_var_length_string())
                        diy::load(bb, destination, selected * type.dtype_size);
                    else
                        serialized_values<true>(bb, type, selected, destination,
                                                &references);
                });
                const char* data = values.get();
                d->data.emplace_back(Dataset::DataTriple {
                    type, memory, file, data, std::move(values)
                });

                log->trace("Read dataset values");
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

        bool has_data;
        diy::load(bb, has_data);
        if (has_data)
        {
            a->data.reset(new char[a->mem_type.dtype_size * a->space.size()]());
            serialized_values<true>(bb, a->mem_type, a->space.size(), a->data.get(), &references);
        }

        o = a;
    }
    else if (type == ObjectType::NamedDtype)
        o = new NamedDtype(name);
    else if (type == ObjectType::HardLink)
    {
        std::uintptr_t target;
        diy::load(bb, target);
        auto* link = new HardLink(name, nullptr);
        hard_links.emplace(link, target);
        o = link;
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
    objects.emplace(token, o);

    for (size_t i = 0; i < n_children; ++i)
        o->add_child(deserialize(bb, hard_links, objects, references, include_data));
    if (auto* file = dynamic_cast<File*>(o))
    {
        file->unlinked.reserve(n_unlinked);
        for (size_t i = 0; i < n_unlinked; ++i)
        {
            auto* object = deserialize(bb, hard_links, objects, references,
                                       include_data);
            object->set_root(file);
            file->unlinked.push_back(object);
        }
    }

    return o;
}
