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
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "-- NamedDtype --\n");
        Object::print(depth);
        // TODO: print named datatype
    }
};

}
