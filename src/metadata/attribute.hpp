#pragma once

#include "file.hpp"
#include "../log-private.hpp"

namespace LowFive
{

struct Attribute: public Object
{

    Attribute(std::string name, hid_t type_id, hid_t space_id):
        Object(ObjectType::Attribute, name), type(type_id),
        space(H5Scopy(space_id), true)                                          {}

    Datatype                        type;
    Dataspace                       space;

    std::unique_ptr<char[]>          data     { nullptr };
    Datatype                        mem_type { type };

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
        const size_t count = space.size();
        auto replacement =
            std::unique_ptr<char[]>(new char[count * type.dtype_size]());
        std::unique_ptr<char[]> current;
        try
        {
            if (data && mem_type_.needs_background(type))
            {
                current.reset(new char[count * type.dtype_size]());
                mem_type.convert(type, count, data.get(), current.get());
                mem_type_.prepare_conversion_background(
                    type, count, current.get(), replacement.get());
            }
            mem_type_.convert(type, count, buf, replacement.get());
        }
        catch (...)
        {
            if (current)
                type.destroy_values(count, current.get());
            type.destroy_values(count, replacement.get());
            throw;
        }
        if (current)
            type.destroy_values(count, current.get());

        unregister_references();
        if (data)
            mem_type.destroy_values(count, data.get());
        data = std::move(replacement);
        mem_type = type;
    }

    void read(Datatype mem_type_, void* buf)
    {
        const size_t count = space.size();
        if (!count) return;
        if (data)
            mem_type.convert(mem_type_, count, data.get(), buf);
        else
            // Like native HDF5, initialize the requested representation
            // directly, including null pointers, without a conversion buffer.
            std::memset(buf, 0, count * mem_type_.dtype_size);
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
