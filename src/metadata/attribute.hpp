#pragma once

#include "file.hpp"
#include "../log-private.hpp"

namespace LowFive
{

struct Attribute: public Object
{

    Attribute(std::string name, hid_t type_id, hid_t space_id):
        Object(ObjectType::Attribute, name), type(type_id), space(space_id)         {}

    Datatype                        type;
    Dataspace                       space;

    std::unique_ptr<char[]>          data     { nullptr };
    Datatype                        mem_type { 0 };

    void unregister_references()
    {
        if (!data) return;
        auto* file = dynamic_cast<File*>(find_root());
        if (!file) return;
        mem_type.visit_references(space.size(), data.get(),
                                  [&](H5R_ref_t* reference)
                                  {
                                      file->references.erase(reference);
                                  });
    }

    ~Attribute() override
    {
        unregister_references();
        if (data && H5Iis_valid(mem_type.id) > 0 && H5Iis_valid(space.id) > 0)
            mem_type.destroy_values(space.size(), data.get());
    }

    void write(Datatype mem_type_, const void* buf)
    {
        unregister_references();
        if (data)
            mem_type.destroy_values(space.size(), data.get());
        data.reset();
        mem_type = type;
        data.reset(new char[space.size() * mem_type.dtype_size]());
        mem_type_.convert(mem_type, space.size(), buf, data.get());
    }

    void read(Datatype mem_type_, void* buf)
    {
        mem_type.convert(mem_type_, space.size(), data.get(), buf);
    }

    void print(int depth) const override
    {
        print_depth(depth);
        fmt::print(stderr, "---- Attribute ---\n");

        print_depth(depth);
        fmt::print(stderr, "type = {}, space = {}, data = {}\n", type, space, fmt::ptr(data));

        Object::print(depth);
    }
};

}
