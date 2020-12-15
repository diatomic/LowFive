#pragma once

namespace LowFive
{

struct Attribute: public Object
{

    Attribute(std::string name, hid_t type_id, hid_t space_id):
        Object(ObjectType::Attribute, name), type(type_id), space(space_id)         {}

    Datatype                        type;
    Dataspace                       space;

    const void*                     data     { nullptr };
    Datatype                        mem_type { 0 };

    void write(Datatype mem_type_, const void* buf)
    {
        mem_type = mem_type_;
        data = buf;
    }

    void print() const override
    {
        fmt::print(stderr, "---- Attribute ---\n");
        Object::print();
        fmt::print(stderr, "type = {}, space = {}, data = {}\n", type, space, fmt::ptr(data));
    }
};

}
