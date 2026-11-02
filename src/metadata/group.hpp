#pragma once

namespace LowFive
{

struct Group : public Object
{
    Hid                             gcpl;                   // hdf5 id of group creation property list

    Group(std::string name, hid_t gcpl_) :
        Object(ObjectType::Group, std::move(name)),
        gcpl(gcpl_ == H5P_DEFAULT ? H5Pcreate(H5P_GROUP_CREATE) :
                                   H5Pcopy(gcpl_), true)     {}

    void print(int depth) const override
    {
        print_depth(depth);
        fmt::print(stderr, "---- Group ----\n");
        Object::print(depth);
        // TODO: print group
    }
};

}
