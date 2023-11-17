#pragma once

namespace LowFive
{

struct NamedDtype : public Object
{
    // TODO: need to initialize this properly first
    // Datatype datatype;

    NamedDtype(std::string name) :
        Object(ObjectType::NamedDtype, name)            {}

    void print(int depth) const override
    {
        print_depth(depth);
        fmt::print(stderr, "-- NamedDtype --\n");
        Object::print(depth);
        // TODO: print named datatype
    }
};

}
