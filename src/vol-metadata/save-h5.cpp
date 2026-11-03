#include "../vol-metadata-private.hpp"
#include "../log-private.hpp"
#include <functional>

namespace LowFive
{

using ReferenceCreator =
    std::function<void(const File::PendingReference&, H5R_ref_t*)>;

struct PreparedValues
{
    const Datatype& type;
    size_t count;
    const void* original;
    std::unique_ptr<char[]> copied;
    bool owns_values = false;

    PreparedValues(
        const File& file, const ReferenceCreator& create_reference,
        const Datatype& type_, size_t count_, const void* values):
        type(type_), count(count_), original(values)
    {
        prepare(file, create_reference, nullptr);
    }

    PreparedValues(
        const File& file, const ReferenceCreator& create_reference,
        const Datatype& type_, size_t count_, const void* values,
        const std::vector<bool>& effective):
        type(type_), count(count_), original(values)
    {
        if (effective.size() != count)
            throw MetadataError("Could not map selected values while saving");
        prepare(file, create_reference, &effective);
    }

    void prepare(
        const File& file, const ReferenceCreator& create_reference,
        const std::vector<bool>* effective)
    {
        std::vector<H5R_ref_t*> source_references;
        std::vector<File::PendingReference> states;
        for (size_t i = 0; i < count; ++i)
        {
            if (effective && !(*effective)[i]) continue;
            type.visit_references(
                1,
                static_cast<char*>(const_cast<void*>(original)) +
                    i * type.dtype_size,
                [&](H5R_ref_t* reference)
                {
                    source_references.push_back(reference);
                    const auto pending = file.references.find(reference);
                    if (pending != file.references.end())
                        states.push_back(pending->second);
                    else
                    {
                        const H5R_ref_t empty {};
                        if (std::memcmp(reference, &empty, sizeof(empty)) == 0)
                            states.emplace_back();
                        else
                            states.push_back(describe_reference(*reference));
                    }
                });
        }
        if (std::none_of(states.begin(), states.end(),
                         [](const File::PendingReference& state)
                         { return state.type != H5R_BADTYPE; }))
            return;

        copied.reset(new char[count * type.dtype_size]());
        owns_values = true;
        try
        {
            std::vector<H5R_ref_t*> copied_references;
            for (size_t i = 0; i < count; ++i)
            {
                if (effective && !(*effective)[i]) continue;
                auto* source =
                    static_cast<const char*>(original) +
                    i * type.dtype_size;
                auto* target = copied.get() + i * type.dtype_size;
                type.copy_values(1, source, target);
                type.visit_references(
                    1, target, [&](H5R_ref_t* reference)
                    {
                        copied_references.push_back(reference);
                    });
            }
            if (source_references.size() != copied_references.size())
                throw MetadataError("Could not map references while saving");

            for (size_t i = 0; i < source_references.size(); ++i)
            {
                if (states[i].type == H5R_BADTYPE) continue;
                auto* reference = copied_references[i];
                if (H5Rget_type(reference) != H5R_BADTYPE &&
                    H5Rdestroy(reference) < 0)
                    throw MetadataError(
                        "Could not replace reference while saving");
                std::memset(reference, 0, sizeof(*reference));
                create_reference(states[i], reference);
            }
        }
        catch (...)
        {
            type.destroy_values(count, copied.get());
            owns_values = false;
            throw;
        }
    }

    ~PreparedValues()
    {
        if (owns_values) type.destroy_values(count, copied.get());
    }

    const void* values() const
    {
        return copied ? copied.get() : original;
    }
};

std::vector<std::vector<bool>>
effective_dataset_values(const Dataset& object)
{
    std::vector<std::vector<bool>> effective(object.data.size());
    Dataspace file_selection(object.space.copy(), true);
    H5Sselect_all(file_selection.id);
    Dataspace memory_selection(file_selection.copy(), true);
    std::vector<bool> covered(memory_selection.size(), false);
    for (size_t i = object.data.size(); i-- > 0;)
    {
        const auto& triple = object.data[i];
        effective[i].resize(triple.memory.size(), false);
        Dataspace destination(
            Dataspace::project_intersection(
                file_selection.id, memory_selection.id, triple.file.id),
            true);
        Dataspace source(
            Dataspace::project_intersection(
                triple.file.id, triple.memory.id, file_selection.id),
            true);
        Dataspace::iterate(
            destination, 1, source, 1,
            [&](size_t to, size_t from, size_t count)
            {
                for (size_t j = 0; j < count; ++j)
                    if (!covered[to + j])
                    {
                        covered[to + j] = true;
                        effective[i][from + j] = true;
                    }
            });
    }
    return effective;
}

void save_dataset_data(
    const ReferenceCreator& create_reference, const Hid& dataset,
    const File& metadata, const Dataset* object)
{
    const auto effective = effective_dataset_values(*object);
    for (size_t i = 0; i < object->data.size(); ++i)
    {
        const auto& x = object->data[i];
        PreparedValues prepared(
            metadata, create_reference, x.type, x.memory.size(), x.data,
            effective[i]);
        const void* values = prepared.values();
        std::vector<const char*> strings;
        if (x.type.is_var_length_string())
        {
            strings.resize(x.memory.size(), nullptr);
            const auto* indices = static_cast<const intptr_t*>(x.data);
            Dataspace::iterate(x.memory, 1, [&](size_t offset, size_t count)
            {
                for (size_t i = offset; i < offset + count; ++i)
                    strings[i] = object->strings[indices[i]].c_str();
            });
            values = strings.data();
        }
        if (H5Dwrite(
                dataset.id, x.type.id, x.memory.id, x.file.id, H5P_DEFAULT,
                values) < 0)
            throw MetadataError("Could not persist dataset data");
    }
}

void require_persistable_type(const Datatype& type)
{
    const auto cls = H5Tget_class(type.id);
    if (cls == H5T_NO_CLASS)
        throw MetadataError("Invalid datatype while saving");
    if (cls == H5T_REFERENCE && !type.is_modern_reference())
        throw MetadataError("Legacy reference values cannot be persisted");
    if (cls == H5T_ARRAY || cls == H5T_VLEN)
        require_persistable_type(Datatype(H5Tget_super(type.id), true));
    else if (cls == H5T_COMPOUND)
        for (int i = 0; i < H5Tget_nmembers(type.id); ++i)
            require_persistable_type(
                Datatype(H5Tget_member_type(type.id, i), true));
}

// Create each object once, independently of its links. This preserves aliases,
// cycles, and link creation order even when a fill references a later object.
struct SavedGraph
{
    const File& metadata;
    const Hid& file;
    std::map<const Object*, Hid> objects;
    std::vector<const Object*> nodes;
    std::unordered_set<const Object*> discovered;
    ReferenceCreator create_reference;

    SavedGraph(const File& metadata_, const Hid& file_):
        metadata(metadata_), file(file_),
        create_reference([this](const File::PendingReference& state,
                                H5R_ref_t* reference)
                         { reference_value(state, reference); })
    {
        discover(&metadata);
        Hid root(H5Gopen(file.id, "/", H5P_DEFAULT), true);
        if (root.id < 0)
            throw MetadataError("Could not open persisted root group");
        objects.emplace(&metadata, std::move(root));
    }

    const Object* hard_target(const HardLink* link) const
    {
        const Object* target = link;
        std::unordered_set<const Object*> visited;
        while (const auto* hard = dynamic_cast<const HardLink*>(target))
        {
            if (!visited.insert(target).second || !hard->target)
                throw MetadataError("Invalid hard-link target while saving");
            target = hard->target;
        }
        if (!dynamic_cast<const Group*>(target) &&
            !dynamic_cast<const Dataset*>(target) &&
            !dynamic_cast<const CommittedDatatype*>(target) &&
            target != &metadata)
            throw MetadataError("Unsupported hard-link target while saving");
        return target;
    }

    void discover(const Object* object)
    {
        if (!discovered.insert(object).second) return;
        if (const auto* link = dynamic_cast<const HardLink*>(object))
        {
            if (!object->children.empty())
                throw MetadataError("Hard-link children cannot be persisted");
            discover(hard_target(link));
            return;
        }
        if (dynamic_cast<const SoftLink*>(object))
        {
            if (!object->children.empty())
                throw MetadataError("Soft-link children cannot be persisted");
            return;
        }
        if (object != &metadata && !dynamic_cast<const Group*>(object) &&
            !dynamic_cast<const Dataset*>(object) &&
            !dynamic_cast<const Attribute*>(object) &&
            !dynamic_cast<const CommittedDatatype*>(object))
            throw MetadataError("Unsupported metadata object while saving");
        nodes.push_back(object);
        if (const auto* dataset = dynamic_cast<const Dataset*>(object))
        {
            require_persistable_type(dataset->type);
            for (const auto& triple : dataset->data)
                require_persistable_type(triple.type);
        }
        else if (const auto* attribute =
                     dynamic_cast<const Attribute*>(object))
        {
            require_persistable_type(attribute->type);
            require_persistable_type(attribute->mem_type);
        }
        for (const auto* child : object->children)
        {
            if (dynamic_cast<const Attribute*>(object) ||
                (object != &metadata && !dynamic_cast<const Group*>(object) &&
                 !dynamic_cast<const Attribute*>(child)))
                throw MetadataError("Unsupported metadata child while saving");
            discover(child);
        }
    }

    const Hid& ensure(const Object* object)
    {
        const auto found = objects.find(object);
        if (found != objects.end()) return found->second;
        if (!discovered.count(object))
            throw MetadataError("Reference target has no persisted hard link");

        Hid saved(0);
        if (const auto* group = dynamic_cast<const Group*>(object))
            saved = Hid(H5Gcreate_anon(file.id, group->gcpl.id, H5P_DEFAULT),
                        true);
        else if (const auto* dataset = dynamic_cast<const Dataset*>(object))
        {
            H5D_fill_value_t status;
            if (H5Pfill_value_defined(dataset->dcpl.id, &status) < 0)
                throw MetadataError("Could not inspect persisted fill value");
            // Native HDF5 1.14 writes scalar reference fills with an invalid
            // disk size. Nested reference fills also undergo shallow copying
            // followed by reclamation during conversion, with no observable
            // ownership transfer on failure. Reject before any DCPL copy.
            if (status == H5D_FILL_VALUE_USER_DEFINED &&
                H5Tdetect_class(dataset->type.id, H5T_REFERENCE) > 0)
                throw MetadataError(
                    "Reference-valued fills cannot be persisted safely "
                    "with HDF5 1.14");
            saved = Hid(H5Dcreate_anon(
                file.id, dataset->type.id, dataset->space.id, dataset->dcpl.id,
                H5P_DEFAULT), true);
        }
        else if (const auto* type =
                     dynamic_cast<const CommittedDatatype*>(object))
        {
            if (type->data.empty())
                throw MetadataError("Committed datatype has no encoded definition");
            saved = Hid(H5Tdecode(type->data.data()), true);
            if (saved.id < 0 ||
                H5Tcommit_anon(file.id, saved.id, H5P_DEFAULT, H5P_DEFAULT) < 0)
                throw MetadataError("Could not persist committed datatype");
        }
        else if (const auto* attribute = dynamic_cast<const Attribute*>(object))
        {
            const auto& parent = ensure(attribute->parent);
            // A forward attribute reference must not reorder tracked attributes.
            for (const auto* sibling : attribute->parent->children)
            {
                if (sibling == attribute) break;
                if (dynamic_cast<const Attribute*>(sibling)) ensure(sibling);
            }
            saved = Hid(H5Acreate(
                parent.id, attribute->name.c_str(), attribute->type.id,
                attribute->space.id, H5P_DEFAULT, H5P_DEFAULT), true);
        }
        else
            throw MetadataError("Unsupported persisted object");
        if (saved.id < 0)
            throw MetadataError("Could not persist metadata object");
        const auto& result = objects.emplace(object, std::move(saved)).first->second;
        return result;
    }

    void reference_value(const File::PendingReference& state,
                         H5R_ref_t* reference)
    {
        if (!state.filename.empty() && state.filename != metadata.name)
            throw MetadataError("Cross-file references cannot be persisted");
        auto* target = const_cast<File&>(metadata).find_token(state.token);
        if (!target)
            throw MetadataError("Could not find reference target while saving");
        const auto& saved = ensure(target);
        herr_t result = -1;
        if (state.type == H5R_OBJECT1 || state.type == H5R_OBJECT2)
            result = H5Rcreate_object(saved.id, ".", H5P_DEFAULT, reference);
        else if (state.type == H5R_DATASET_REGION1 ||
                 state.type == H5R_DATASET_REGION2)
            result = H5Rcreate_region(saved.id, ".", state.region.id,
                                     H5P_DEFAULT, reference);
        else if (state.type == H5R_ATTR)
        {
            const auto* attribute =
                const_cast<Object*>(target)->attribute(state.attribute);
            if (!attribute)
                throw MetadataError("Could not find reference attribute while saving");
            ensure(attribute);
            result = H5Rcreate_attr(saved.id, ".", state.attribute.c_str(),
                                   H5P_DEFAULT, reference);
        }
        if (result < 0)
            throw MetadataError("Could not reconstruct reference while saving");
    }

    void save()
    {
        for (const auto* object : nodes) ensure(object);
        for (const auto* object : nodes)
        {
            const auto& parent = objects.at(object);
            for (const auto* child : object->children)
            {
                if (dynamic_cast<const Attribute*>(child)) continue;
                herr_t result;
                if (const auto* soft = dynamic_cast<const SoftLink*>(child))
                    result = H5Lcreate_soft(
                        soft->target.c_str(), parent.id, soft->name.c_str(),
                        H5P_DEFAULT, H5P_DEFAULT);
                else
                {
                    const auto* hard = dynamic_cast<const HardLink*>(child);
                    const auto* target = hard ? hard_target(hard) : child;
                    result = H5Olink(objects.at(target).id, parent.id,
                                     child->name.c_str(), H5P_DEFAULT,
                                     H5P_DEFAULT);
                }
                if (result < 0)
                    throw MetadataError("Could not persist link");
            }
        }
        for (const auto* object : nodes)
        {
            const auto& saved = objects.at(object);
            if (const auto* dataset = dynamic_cast<const Dataset*>(object))
                save_dataset_data(create_reference, saved, metadata, dataset);
            else if (const auto* attribute =
                         dynamic_cast<const Attribute*>(object))
            {
                if (!attribute->data) continue;
                PreparedValues prepared(
                    metadata, create_reference, attribute->mem_type,
                    attribute->space.size(), attribute->data.get());
                if (H5Awrite(saved.id, attribute->mem_type.id,
                             prepared.values()) < 0)
                    throw MetadataError("Could not persist attribute data");
            }
        }
        for (auto& entry : objects)
        {
            if (H5Idec_ref(entry.second.id) < 0)
                throw MetadataError("Could not close persisted object");
            entry.second.id = 0;
        }
        if (H5Fflush(file.id, H5F_SCOPE_GLOBAL) < 0)
            throw MetadataError("Could not flush persisted file");
    }
};

}

void
LowFive::save(const File& f, std::string filename)
{
    auto log = LowFive::get_logger();

    log->trace("Saving {} to {}", f.name, filename);

    Hid fid(H5Fcreate(filename.c_str(),
                      H5F_ACC_TRUNC,
                      f.fcpl.id,
                      H5P_DEFAULT), true);
    if (fid.id < 0)
        throw MetadataError("Could not create persisted file");

    {
        SavedGraph graph(f, fid);
        graph.save();
    }
    const auto saved_file = std::exchange(fid.id, 0);
    if (H5Fclose(saved_file) < 0)
        throw MetadataError("Could not close persisted file");
}
