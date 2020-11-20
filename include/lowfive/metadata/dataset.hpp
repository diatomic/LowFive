#pragma once

namespace LowFive
{

struct Dataset : public Object
{
    struct DataTriple
    {
        Dataspace memory;
        Dataspace file;
        const void* data;
    };

    Datatype                        datatype;
    std::vector<DataTriple>         data;


    Dataset(std::string name, hid_t dtype_id) :
        Object(ObjectType::Dataset, name), datatype(dtype_id)
    {}

    void write(Dataspace memory, Dataspace file, const void* buf)
    {
        data.emplace_back(DataTriple { memory, file, buf });
    }

    void print() override
    {
        fmt::print(stderr, "---- Dataset ---\n");
        Object::print();
        fmt::print(stderr, "datatype = {}\n", datatype.description());
        for (auto& d : data)
            fmt::print("memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
    }
};

}
