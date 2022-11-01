#pragma once

namespace LowFive
{

struct Group : public Object
{
    Hid                             gcpl;                   // hdf5 id of group creation property list

    Group(std::string name, Hid gcpl_) :
        Object(ObjectType::Group, name), gcpl(gcpl_)        {}

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Group ----\n");
        Object::print(depth);
        // TODO: print group
    }
};

}
