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

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own):
        Object(ObjectType::Dataset, name), type(dtype_id), space(space_id), ownership(own)
    {}

    void write(Datatype type, Dataspace memory, Dataspace file, const void* buf)
    {
        if (ownership == Ownership::lowfive)
        {
            size_t nbytes   = file.size() * type.dtype_size;
            char* p         = new char[nbytes];
            std::memcpy(p, buf, nbytes);
            data.emplace_back(DataTriple { type, memory, file, p, std::unique_ptr<char[]>(p) });
        }
        else
            data.emplace_back(DataTriple { type, memory, file, buf, std::unique_ptr<char[]>(nullptr) });
    }

    void print() const override
    {
        fmt::print(stderr, "---- Dataset ---\n");
        Object::print();
        fmt::print(stderr, "type = {}, space = {}, ownership = {}\n", type, space, ownership);
        for (auto& d : data)
            fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
    }
};

struct RemoteDataset : public Object
{
    RemoteDataset(std::string name):
        Object(ObjectType::Dataset, name, false)    // false: remote datasets don't strip path from their name
    {}

    void* query = nullptr;
};

}
