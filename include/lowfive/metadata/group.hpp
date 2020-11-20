#pragma once

namespace LowFive
{

struct Group : public Object
{
    Group(std::string name) :
        Object(ObjectType::Group, name)                 {}

    void print() override
    {
        fmt::print(stderr, "---- Group ----\n");
        Object::print();
        // TODO: print group
    }
};

}