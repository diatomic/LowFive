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

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own, Hid dcpl_):
        Object(ObjectType::Dataset, name), type(dtype_id), space(space_id), ownership(own), dcpl(dcpl_)
    {}

    void write(Datatype type, Dataspace memory, Dataspace file, const void* buf)
    {
        if (ownership == Ownership::lowfive)
        {
            size_t nbytes   = (memory.id ? memory.size() : space.size()) * type.dtype_size;
            char* p         = new char[nbytes];
            std::memcpy(p, buf, nbytes);
            data.emplace_back(DataTriple { type, memory, file, p, std::unique_ptr<char[]>(p) });
        }
        else
            data.emplace_back(DataTriple { type, memory, file, buf, std::unique_ptr<char[]>(nullptr) });
    }

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Dataset ---\n");

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "type = {}, space = {}, ownership = {}\n", type, space, ownership);

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        for (auto& d : data)
            fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));

        Object::print(depth);
    }
};

}
