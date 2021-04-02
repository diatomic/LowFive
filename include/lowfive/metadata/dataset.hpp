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
    };

    using DataTriples = std::vector<DataTriple>;

    Datatype                        type;
    Dataspace                       space;
    DataTriples                     data;


    Dataset(std::string name, hid_t dtype_id, hid_t space_id):
        Object(ObjectType::Dataset, name), type(dtype_id), space(space_id)
    {}

    void write(Datatype type, Dataspace memory, Dataspace file, const void* buf)
    {
        data.emplace_back(DataTriple { type, memory, file, buf });
    }

    void print() const override
    {
        fmt::print(stderr, "---- Dataset ---\n");
        Object::print();
        fmt::print(stderr, "type = {}, space = {}\n", type, space);
        for (auto& d : data)
            fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
    }
};

struct RemoteDataset : public Object
{
    RemoteDataset(std::string name):
        Object(ObjectType::Dataset, name)
    {}

    void* index = nullptr;
};

}
