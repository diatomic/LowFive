#pragma once

namespace LowFive
{

struct NamedDtype : public Object
{
    Datatype                        datatype;

    NamedDtype(std::string name) :
        Object(ObjectType::NamedDtype, name)            {}

    void print() override
    {
        fmt::print(stderr, "-- NamedDtype --\n");
        Object::print();
        // TODO: print named datatype
    }
};

}
