#pragma once

#include "../log-private.hpp"

namespace LowFive
{

struct Attribute: public Object
{

    Attribute(std::string name, hid_t type_id, hid_t space_id):
        Object(ObjectType::Attribute, name), type(type_id), space(space_id)         {}

    Datatype                        type;
    Dataspace                       space;

    std::unique_ptr<char>           data     { nullptr };
    Datatype                        mem_type { 0 };

    std::vector<std::string>        strings;
    hvl_t                           vlen     { 0, nullptr };

    void write(Datatype mem_type_, const void* buf)
    {
        auto log = get_logger();
        mem_type = mem_type_;

        if (type.dtype_class == DatatypeClass::VarLen)
        {
            hvl_t* val = (hvl_t*) buf;
            vlen.len = val->len;
            auto base_type = Datatype(H5Tget_super(mem_type.id));
            log->warn("Writing an attribute with variable length = {}, base type = {}", val->len, base_type);

            size_t nbytes = vlen.len * base_type.dtype_size;
            data = std::unique_ptr<char>(new char[nbytes]);
            vlen.p = data.get();
            std::memcpy(static_cast<void*>(data.get()), val->p, nbytes);
        } else if (!mem_type.is_var_length_string())
        {
            size_t nbytes = space.size() * mem_type.dtype_size;
            data = std::unique_ptr<char>(new char[nbytes]);
            std::memcpy(static_cast<void*>(data.get()), buf, nbytes);
        } else      // varlen string
        {
            for (size_t i = 0; i < space.size(); ++i)
            {
                auto c_str = ((const char**) buf)[i];
                if (c_str)
                    strings.push_back(std::string(c_str));
                else
                    strings.emplace_back();     // empty for null
            }
        }
    }

    void read(Datatype mem_type_, void* buf)
    {
        log_assert(mem_type.equal(mem_type_), "Currently only know how to read the same datatype as written");

        if (type.dtype_class == DatatypeClass::VarLen)
        {
            std::memcpy(buf, static_cast<void*>(&vlen), sizeof(vlen));
        } else if (!mem_type.is_var_length_string())
        {
            size_t nbytes = space.size() * mem_type.dtype_size;
            std::memcpy(buf, static_cast<void*>(data.get()), nbytes);
        } else
        {
            for (size_t i = 0; i < space.size(); ++i)
            {
                if (!strings[i].empty())
                {
                    // presumably HDF5 will manage this string and reclaim the
                    // memory; I'm unclear on how this works, so it's a potential
                    // memory leak
                    char *cstr = new char[strings[i].length() + 1];
                    strcpy(cstr, strings[i].c_str());
                    ((const char**) buf)[i] = cstr;
                } else
                    ((const char**) buf)[i] = NULL;
            }
        }
    }

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Attribute ---\n");

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "type = {}, space = {}, data = {}, strings.size() = {}\n", type, space, fmt::ptr(data), strings.size());

        Object::print(depth);
    }
};

}
