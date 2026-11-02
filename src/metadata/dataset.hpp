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

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own,
            Hid dcpl_, Hid dapl_, bool is_passthru_, bool is_memory_):
            Object(ObjectType::Dataset, name), type(dtype_id),
            space(H5Scopy(space_id), true), ownership(own),
            dcpl(dcpl_.id == H5P_DEFAULT ? H5Pcreate(H5P_DATASET_CREATE) :
                                          H5Pcopy(dcpl_.id), true),
            dapl(dapl_.id == H5P_DEFAULT ? H5Pcreate(H5P_DATASET_ACCESS) :
                                          H5Pcopy(dapl_.id), true),
            is_passthru(is_passthru_),
            is_memory(is_memory_)
    {
        if (type.is_var_length_string())
            ownership = Ownership::lowfive;     // strings must be copied into the serializable string table
    }

    void set_extent(const hsize_t* size)
    {
        bool shrinking = false;
        for (int i = 0; i < space.dim; ++i)
            shrinking = shrinking || size[i] < space.dims[i];

        space.set_extent(size);
        H5Sselect_all(space.id);
        if (!shrinking) return;

        for (auto& triple : data)
        {
            const int rank = H5Sget_simple_extent_ndims(triple.file.id);
            std::vector<hsize_t> file_dims(rank);
            H5Sget_simple_extent_dims(triple.file.id, file_dims.data(), nullptr);
            std::vector<hsize_t> start(rank, 0);
            std::vector<hsize_t> retained_dims(rank);
            bool empty = false;
            for (int i = 0; i < rank; ++i)
            {
                retained_dims[i] = std::min(file_dims[i], size[i]);
                empty = empty || retained_dims[i] == 0;
            }

            Dataspace retained(triple.file.copy(), true);
            const herr_t selected =
                empty ? H5Sselect_none(retained.id) :
                        H5Sselect_hyperslab(retained.id, H5S_SELECT_SET,
                                            start.data(), nullptr,
                                            retained_dims.data(), nullptr);
            if (selected < 0)
                throw MetadataError("Could not select retained dataset extent");

            hid_t file_id = Dataspace::project_intersection(
                triple.file.id, triple.file.id, retained.id);
            hid_t memory_id = Dataspace::project_intersection(
                triple.file.id, triple.memory.id, retained.id);
            if (file_id < 0 || memory_id < 0)
            {
                if (file_id >= 0) H5Sclose(file_id);
                if (memory_id >= 0) H5Sclose(memory_id);
                throw MetadataError("Could not discard data outside dataset extent");
            }
            triple.file = Dataspace(file_id, true);
            triple.memory = Dataspace(memory_id, true);
        }
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

        auto owned = std::unique_ptr<char[]>(
            new char[memory_ds.size() * type.dtype_size]());
        char* values = owned.get();
        const bool preserve_background =
            memory_type.needs_background(type);
        std::unique_ptr<char[]> current;
        if (preserve_background)
        {
            current.reset(new char[memory_ds.size() * type.dtype_size]());
            read(type.id, memory_ds.id, file_ds.id, current.get());
        }
        try
        {
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
                        memory_type.convert(
                            type, count,
                            static_cast<const char*>(buf) +
                                offset * memory_type.dtype_size,
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
                {
                    if (current)
                        memory_type.prepare_conversion_background(
                            type, count,
                            current.get() + offset * type.dtype_size,
                            values + offset * type.dtype_size);
                    memory_type.convert(
                        type, count,
                        static_cast<const char*>(buf) +
                            offset * memory_type.dtype_size,
                        values + offset * type.dtype_size);
                }
            });
        }
        catch (...)
        {
            if (!type.is_var_length_string())
            {
                if (current)
                    type.destroy_values(memory_ds.size(), current.get());
                type.destroy_values(memory_ds.size(), values);
            }
            throw;
        }
        if (current && !type.is_var_length_string())
            type.destroy_values(memory_ds.size(), current.get());
        data.emplace_back(DataTriple {
            type, memory_ds, file_ds, values, std::move(owned)
        });
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
        const bool indirect = requested.owns_indirect_values();
        std::vector<bool> initialized(memory.size(), false);

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
                    {
                        const size_t destination_index = to + i;
                        if (initialized[destination_index]) continue;
                        selected_strings[destination_index] =
                            strings[indices[from + i]].c_str();
                        initialized[destination_index] = true;
                    }
                }
                else if (!indirect)
                {
                    dt.type.convert(requested, count,
                                    static_cast<const char*>(dt.data) + from * dt.type.dtype_size,
                                    static_cast<char*>(buf) + to * requested.dtype_size);
                    for (size_t i = 0; i < count; ++i)
                        initialized[to + i] = true;
                }
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
        if (indirect)
            for (auto dt = data.rbegin(); dt != data.rend(); ++dt)
                read_triple(*dt);
        else
            for (auto& dt : data)
                read_triple(dt);

        if (type.is_var_length_string())
            Dataspace::iterate(memory, 1, [&](size_t offset, size_t count)
            {
                for (size_t i = offset; i < offset + count; ++i)
                    if (selected_strings[i])
                        type.convert(requested, 1, &selected_strings[i],
                                     static_cast<char*>(buf) + i * requested.dtype_size);
            });

        H5D_fill_value_t fill_status = H5D_FILL_VALUE_DEFAULT;
        if (dcpl.id != H5P_DEFAULT &&
            H5Pfill_value_defined(dcpl.id, &fill_status) < 0)
            throw MetadataError("Could not query dataset fill value");
        if (fill_status != H5D_FILL_VALUE_UNDEFINED)
        {
            std::vector<char> fill(type.dtype_size, 0);
            if (dcpl.id != H5P_DEFAULT &&
                H5Pget_fill_value(dcpl.id, type.id, fill.data()) < 0)
                throw MetadataError("Could not read dataset fill value");
            try
            {
                Dataspace::iterate(memory, 1, [&](size_t offset, size_t count)
                {
                    for (size_t i = offset; i < offset + count; ++i)
                        if (!initialized[i])
                            type.convert(
                                requested, 1, fill.data(),
                                static_cast<char*>(buf) +
                                    i * requested.dtype_size);
                });
            }
            catch (...)
            {
                if (type.owns_indirect_values())
                    type.destroy_values(1, fill.data());
                throw;
            }
            if (type.owns_indirect_values())
                type.destroy_values(1, fill.data());
        }
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
