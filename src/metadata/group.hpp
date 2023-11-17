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
        print_depth(depth);
        fmt::print(stderr, "---- Group ----\n");
        Object::print(depth);
        // TODO: print group
    }
};

}
