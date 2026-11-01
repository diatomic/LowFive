#include "../metadata.hpp"
#include "../log-private.hpp"

namespace LowFive
{

std::string saved_reference_path(
    const File& file, const File::PendingReference& state)
{
    if (!state.filename.empty() && state.filename != file.name)
        throw MetadataError("Cross-file references cannot be persisted");
    auto& metadata = const_cast<File&>(file);
    Object* target = metadata.find_token(state.token);
    if (!target)
        throw MetadataError("Could not find reference target while saving");
    auto path = metadata.reachable_path(target);
    if (path.empty())
        throw MetadataError("Reference target has no persisted path");
    return path;
}

void create_saved_reference(
    hid_t file_id, const File& file, const File::PendingReference& state,
    H5R_ref_t* reference)
{
    const auto path = saved_reference_path(file, state);
    herr_t result = -1;
    if (state.type == H5R_OBJECT1 || state.type == H5R_OBJECT2)
        result = H5Rcreate_object(
            file_id, path.c_str(), H5P_DEFAULT, reference);
    else if (state.type == H5R_DATASET_REGION1 ||
             state.type == H5R_DATASET_REGION2)
        result = H5Rcreate_region(
            file_id, path.c_str(), state.region.id, H5P_DEFAULT, reference);
    else if (state.type == H5R_ATTR)
        result = H5Rcreate_attr(
            file_id, path.c_str(), state.attribute.c_str(), H5P_DEFAULT,
            reference);
    if (result < 0)
        throw MetadataError("Could not reconstruct reference while saving");
}

struct PreparedValues
{
    const Datatype& type;
    size_t count;
    const void* original;
    std::unique_ptr<char[]> copied;
    bool owns_values = false;

    PreparedValues(
        const File& file, hid_t file_id, const Datatype& type_, size_t count_,
        const void* values):
        type(type_), count(count_), original(values)
    {
        prepare(file, file_id, nullptr);
    }

    PreparedValues(
        const File& file, hid_t file_id, const Datatype& type_, size_t count_,
        const void* values, const std::vector<bool>& effective):
        type(type_), count(count_), original(values)
    {
        if (effective.size() != count)
            throw MetadataError("Could not map selected values while saving");
        prepare(file, file_id, &effective);
    }

    void prepare(
        const File& file, hid_t file_id,
        const std::vector<bool>* effective)
    {
        std::vector<H5R_ref_t*> source_references;
        bool has_pending = false;
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
                    has_pending =
                        has_pending ||
                        file.references.find(reference) !=
                            file.references.end();
                });
        }
        if (!has_pending) return;

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
                auto pending = file.references.find(source_references[i]);
                if (pending == file.references.end()) continue;
                auto* reference = copied_references[i];
                if (H5Rget_type(reference) != H5R_BADTYPE &&
                    H5Rdestroy(reference) < 0)
                    throw MetadataError(
                        "Could not replace reference while saving");
                std::memset(reference, 0, sizeof(*reference));
                create_saved_reference(
                    file_id, file, pending->second, reference);
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
    const Hid& file, const Hid& dataset, const File& metadata,
    const Dataset* object)
{
    const auto effective = effective_dataset_values(*object);
    for (size_t i = 0; i < object->data.size(); ++i)
    {
        const auto& x = object->data[i];
        PreparedValues prepared(
            metadata, file.id, x.type, x.memory.size(), x.data, effective[i]);
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

void save_structure(const Hid& parent, const Object* object)
{
    for (const Object* child : object->children)
    {
        if (const auto* group = dynamic_cast<const Group*>(child))
        {
            Hid saved(H5Gcreate(
                parent.id, group->name.c_str(), H5P_DEFAULT, H5P_DEFAULT,
                H5P_DEFAULT), true);
            if (saved.id < 0)
                throw MetadataError("Could not persist group");
            save_structure(saved, group);
        }
        else if (const auto* dataset = dynamic_cast<const Dataset*>(child))
        {
            Hid saved(H5Dcreate(
                parent.id, dataset->name.c_str(), dataset->type.id,
                dataset->space.id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                true);
            if (saved.id < 0)
                throw MetadataError("Could not persist dataset");
            save_structure(saved, dataset);
        }
        else if (const auto* attribute = dynamic_cast<const Attribute*>(child))
        {
            Hid saved(H5Acreate(
                parent.id, attribute->name.c_str(), attribute->type.id,
                attribute->space.id, H5P_DEFAULT, H5P_DEFAULT), true);
            if (saved.id < 0)
                throw MetadataError("Could not persist attribute");
        }
    }
}

void save_values(const Hid& file, const Hid& parent, const File& metadata,
                 const Object* object)
{
    for (const Object* child : object->children)
    {
        if (const auto* group = dynamic_cast<const Group*>(child))
        {
            Hid saved(H5Gopen(parent.id, group->name.c_str(), H5P_DEFAULT),
                      true);
            if (saved.id < 0)
                throw MetadataError("Could not reopen persisted group");
            save_values(file, saved, metadata, group);
        }
        else if (const auto* dataset = dynamic_cast<const Dataset*>(child))
        {
            Hid saved(H5Dopen(parent.id, dataset->name.c_str(), H5P_DEFAULT),
                      true);
            if (saved.id < 0)
                throw MetadataError("Could not reopen persisted dataset");
            save_dataset_data(file, saved, metadata, dataset);
            save_values(file, saved, metadata, dataset);
        }
        else if (const auto* attribute =
                     dynamic_cast<const Attribute*>(child))
        {
            if (!attribute->data) continue;
            Hid saved(H5Aopen(parent.id, attribute->name.c_str(), H5P_DEFAULT),
                      true);
            if (saved.id < 0)
                throw MetadataError("Could not reopen persisted attribute");
            PreparedValues prepared(
                metadata, file.id, attribute->mem_type,
                attribute->space.size(), attribute->data.get());
            if (H5Awrite(
                    saved.id, attribute->mem_type.id, prepared.values()) < 0)
                throw MetadataError("Could not persist attribute data");
        }
    }
}

}

void
LowFive::save(const File& f, std::string filename)
{
    auto log = LowFive::get_logger();

    log->trace("Saving {} to {}", f.name, filename);

    Hid fid(H5Fcreate(filename.c_str(),
                      H5F_ACC_TRUNC,
                      H5P_DEFAULT,
                      H5P_DEFAULT), true);
    if (fid.id < 0)
        throw MetadataError("Could not create persisted file");

    save_structure(fid, &f);
    save_values(fid, fid, f, &f);
}
