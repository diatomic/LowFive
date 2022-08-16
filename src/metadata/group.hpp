#pragma once

namespace LowFive
{

struct Group : public Object
{
    Group(std::string name) :
        Object(ObjectType::Group, name)                 {}

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
