#pragma once

namespace LowFive
{

struct NamedDtype : public Object
{
    // TODO: need to initialize this properly first
    // Datatype datatype;

    NamedDtype(std::string name) :
        Object(ObjectType::NamedDtype, name)            {}

    void print() const override
    {
        fmt::print(stderr, "-- NamedDtype --\n");
        Object::print();
        // TODO: print named datatype
    }
};

}
