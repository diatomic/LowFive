#pragma once

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

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own, Hid dcpl_, Hid dapl_):
        Object(ObjectType::Dataset, name), type(dtype_id), space(space_id), ownership(own), dcpl(dcpl_), dapl(dapl_)
    {}

    void write(Datatype type, Dataspace memory, Dataspace file, const void* buf)
    {
        // make copies of the file and memory dataspaces here to keep their boxes as they were at this moment
        Dataspace& memory_ds_1 = (memory.id != H5S_ALL ? memory : space);
        Dataspace memory_ds = Dataspace(memory_ds_1.copy(), true);

        Dataspace& file_ds_1 = (file.id != H5S_ALL ? file : space);
        Dataspace file_ds = Dataspace(file_ds_1.copy(), true);

        if (ownership == Ownership::lowfive)
        {
            size_t nbytes   = memory_ds.size() * type.dtype_size;
            char* p         = new char[nbytes];
            std::memcpy(p, buf, nbytes);
            data.emplace_back(DataTriple { type, memory_ds, file_ds, p, std::unique_ptr<char[]>(p) });
        }
        else
            data.emplace_back(DataTriple { type, memory_ds, file_ds, buf, std::unique_ptr<char[]>(nullptr) });
    }

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Dataset ---\n");

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "type = {}, space = {}, ownership = {}\n", type, space, ownership);

        if (data.size())
        {
            for (auto i = 0; i < depth; i++)
                fmt::print(stderr, "    ");
            for (auto& d : data)
                fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
        }

        Object::print(depth);
    }
};

}
