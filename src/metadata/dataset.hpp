#pragma once

#include "file.hpp"

namespace LowFive
{

struct Dataset : public Object
{
    struct DataTriple
    {
        Datatype    type;       // memory type
        Dataspace   memory;
        Dataspace   file;
        const void* data;
        std::unique_ptr<char[]> owned_data;
    };

    enum Ownership
    {
        user,                   // lowfive has only shallow pointer
        lowfive                 // lowfive deep copies data
    };

    using DataTriples = std::vector<DataTriple>;

    Datatype                        type;
    Dataspace                       space;
    DataTriples                     data;
    Ownership                       ownership;
    Hid                             dcpl;                   // hdf5 id of dataset creation property list
    Hid                             dapl;                   // hdf5 id of dataset access property list
    bool                            is_passthru {false };   // true if dataset that should go to disk
    bool                            is_memory {false };     // true if dataset should be kept in memory

    std::vector<std::string>        strings;

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own, Hid dcpl_, Hid dapl_, bool is_passthru_, bool is_memory_):
            Object(ObjectType::Dataset, name), type(dtype_id), space(space_id), ownership(own), dcpl(dcpl_), dapl(dapl_),
            is_passthru(is_passthru_),
            is_memory(is_memory_)
    {
        if (type.is_var_length_string())
            ownership = Ownership::lowfive;     // strings must be copied into the serializable string table
    }

    void unregister_references()
    {
        auto* file = dynamic_cast<File*>(find_root());
        if (!file) return;
        for (auto& triple : data)
            if (triple.owned_data && H5Iis_valid(triple.type.id) > 0 &&
                H5Iis_valid(triple.memory.id) > 0)
                triple.type.visit_references(
                    triple.memory.size(), triple.owned_data.get(),
                    [&](H5R_ref_t* reference)
                    {
                        file->references.erase(reference);
                    });
    }

    ~Dataset() override
    {
        unregister_references();
        for (auto& triple : data)
            if (triple.owned_data && H5Iis_valid(triple.type.id) > 0 &&
                H5Iis_valid(triple.memory.id) > 0 && !triple.type.is_var_length_string())
                triple.type.destroy_values(triple.memory.size(), triple.owned_data.get());
    }

    void write(Datatype memory_type, Dataspace memory, Dataspace file, const void* buf)
    {
        Dataspace file_ds(file.id != H5S_ALL ? file.copy() : space.copy(), true);
        if (file.id == H5S_ALL)
            H5Sselect_all(file_ds.id);
        Dataspace memory_ds(memory.id != H5S_ALL ? memory.copy() : file_ds.copy(), true);

        if (ownership == Ownership::user && memory_type.equal(type))
        {
            data.emplace_back(DataTriple { type, memory_ds, file_ds, buf, nullptr });
            return;
        }

        auto owned = std::unique_ptr<char[]>(new char[memory_ds.size() * type.dtype_size]());
        char* values = owned.get();
        Dataspace::iterate(memory_ds, 1, [&](size_t offset, size_t count)
        {
            if (type.is_var_length_string())
            {
                auto* indices = reinterpret_cast<intptr_t*>(values);
                std::vector<char*> converted;
                const char* const* input = nullptr;
                if (memory_type.is_var_length_string())
                    input = static_cast<const char* const*>(buf) + offset;
                else
                {
                    converted.resize(count, nullptr);
                    memory_type.convert(type, count,
                                        static_cast<const char*>(buf) + offset * memory_type.dtype_size,
                                        converted.data());
                    input = converted.data();
                }
                for (size_t i = 0; i < count; ++i)
                {
                    strings.emplace_back(input[i] ? input[i] : "");
                    indices[offset + i] = strings.size() - 1;
                }
                if (!converted.empty())
                    type.destroy_values(count, converted.data());
            }
            else
                memory_type.convert(type, count,
                                    static_cast<const char*>(buf) + offset * memory_type.dtype_size,
                                    values + offset * type.dtype_size);
        });
        data.emplace_back(DataTriple { type, memory_ds, file_ds, values, std::move(owned) });
    }
    template<class Visitor>
    void visit_read_references(hid_t memory_space_id, hid_t file_space_id,
                               Visitor&& visit)
    {
        Dataspace file_selection(
            file_space_id != H5S_ALL ? H5Scopy(file_space_id) : space.copy(),
            true);
        if (file_space_id == H5S_ALL)
            H5Sselect_all(file_selection.id);
        Dataspace memory_selection(
            memory_space_id != H5S_ALL ? H5Scopy(memory_space_id) :
                                         file_selection.copy(),
            true);
        std::vector<bool> covered(memory_selection.size(), false);
        for (auto triple = data.rbegin(); triple != data.rend(); ++triple)
        {
            Dataspace destination(
                Dataspace::project_intersection(
                    file_selection.id, memory_selection.id, triple->file.id),
                true);
            Dataspace source(
                Dataspace::project_intersection(
                    triple->file.id, triple->memory.id, file_selection.id),
                true);
            Dataspace::iterate(
                destination, 1, source, 1,
                [&](size_t to, size_t from, size_t count)
                {
                    for (size_t i = 0; i < count; ++i)
                    {
                        const size_t destination_index = to + i;
                        if (covered[destination_index]) continue;
                        const auto* value =
                            static_cast<const char*>(triple->data) +
                            (from + i) * triple->type.dtype_size;
                        triple->type.visit_references(
                            1, const_cast<char*>(value), visit);
                        covered[destination_index] = true;
                    }
                });
        }
    }


    void read(hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, void* buf)
    {
        Datatype requested(mem_type_id);
        Dataspace file(file_space_id != H5S_ALL ? H5Scopy(file_space_id) : space.copy(), true);
        if (file_space_id == H5S_ALL)
            H5Sselect_all(file.id);
        Dataspace memory(mem_space_id != H5S_ALL ? H5Scopy(mem_space_id) : file.copy(), true);
        std::vector<const char*> selected_strings;
        if (type.is_var_length_string())
            selected_strings.resize(memory.size(), nullptr);
        std::vector<bool> initialized;
        if (!type.is_var_length_string() && requested.owns_indirect_values())
            initialized.resize(memory.size(), false);

        auto read_triple = [&](auto& dt)
        {
            Dataspace dst(Dataspace::project_intersection(file.id, memory.id, dt.file.id), true);
            Dataspace src(Dataspace::project_intersection(dt.file.id, dt.memory.id, file.id), true);
            Dataspace::iterate(dst, 1, src, 1, [&](size_t to, size_t from, size_t count)
            {
                if (dt.type.is_var_length_string())
                {
                    const auto* indices = static_cast<const intptr_t*>(dt.data);
                    for (size_t i = 0; i < count; ++i)
                        selected_strings[to + i] = strings[indices[from + i]].c_str();
                }
                else if (initialized.empty())
                    dt.type.convert(requested, count,
                                    static_cast<const char*>(dt.data) + from * dt.type.dtype_size,
                                    static_cast<char*>(buf) + to * requested.dtype_size);
                else
                {
                    for (size_t i = 0; i < count; ++i)
                    {
                        const size_t destination_index = to + i;
                        if (initialized[destination_index]) continue;
                        char* destination = static_cast<char*>(buf) +
                                            destination_index * requested.dtype_size;
                        dt.type.convert(requested, 1,
                                        static_cast<const char*>(dt.data) +
                                            (from + i) * dt.type.dtype_size,
                                        destination);
                        initialized[destination_index] = true;
                    }
                }
            });
        };
        if (initialized.empty())
            for (auto& dt : data) read_triple(dt);
        else
            for (auto dt = data.rbegin(); dt != data.rend(); ++dt)
                read_triple(*dt);
        if (type.is_var_length_string())
            Dataspace::iterate(memory, 1, [&](size_t offset, size_t count)
            {
                for (size_t i = offset; i < offset + count; ++i)
                    if (selected_strings[i])
                        type.convert(requested, 1, &selected_strings[i],
                                     static_cast<char*>(buf) + i * requested.dtype_size);
            });
    }

    void print(int depth) const override
    {
        print_depth(depth);
        fmt::print(stderr, "---- Dataset ---\n");

        print_depth(depth);
        fmt::print(stderr, "type = {}, space = {}, ownership = {}\n", type, space, ownership);

        if (data.size())
        {
            print_depth(depth);
            for (auto& d : data)
                fmt::print(stderr, "memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
        }

        Object::print(depth);
    }
};

}
