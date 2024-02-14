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

    void write(Datatype mem_type_, const void* buf)
    {
        auto log = get_logger();
        mem_type = mem_type_;

        if (mem_type.dtype_class == DatatypeClass::Compound)
        {
            hid_t type_id = mem_type_.id;

            log->trace("Attribute::write Compound");

            auto nmembers = H5Tget_nmembers(type_id);
            log->trace("Attribute::write nmembers = {}", nmembers);

            size_t total_size = 0;
            for(int member_idx = 0; member_idx < nmembers; ++member_idx) {
                total_size += H5Tget_size(H5Tget_member_type(type_id, member_idx));
            }

            log->trace("Attribute::write Compound total_size = {}, type size = {}", total_size, H5Tget_size(type_id));

            data = std::unique_ptr<char>(new char[total_size]);

            for(int member_idx = 0; member_idx < nmembers; ++member_idx)
            {
                auto member_offset = H5Tget_member_offset(type_id, member_idx);
                H5T_class_t member_class = H5Tget_member_class(type_id, member_idx);
                size_t member_size = H5Tget_size(H5Tget_member_type(type_id, member_idx));

                log->trace("Attribute::write compound, member_idx = {}, member_offset = {}, size = {}", member_idx, member_offset, member_size);

                if (member_class == H5T_REFERENCE)
                {
                    assert(sizeof(H5R_ref_t ) == member_size);
                    H5R_ref_t* src = (H5R_ref_t*)((char*)buf + member_offset);
                    H5R_ref_t* dest = (H5R_ref_t*)(data.get() + member_offset);
                    H5Rcopy(src, dest);
                } else if (member_class == H5T_INTEGER || member_class == H5T_FLOAT)
                {
                    char* dest = data.get() + member_offset;
                    char* src = (char*)buf + member_offset;
                    std::memcpy(dest, src, member_size);
                } else
                {
                    throw MetadataError("unsupported member class in compound type");
                }
            }

        }
        else if (mem_type.dtype_class == DatatypeClass::VarLen)
        {
            log->warn("Working with variable-length array, but destructor not implemented. This will leak memory.");
            log_assert(sizeof(hvl_t) == mem_type.dtype_size, "dtype_size must match sizeof(hvl_t); otherwise read will be incorrect");

            hvl_t* buf_hvl = (hvl_t*) buf;
            size_t nbytes = space.size() * sizeof(hvl_t);
            data = std::unique_ptr<char>(new char[nbytes]);

            hvl_t* data_hvl = (hvl_t*) data.get();
            auto base_type = Datatype(H5Tget_super(mem_type.id));

            for (size_t i = 0; i < space.size(); ++i)
            {
                auto len = buf_hvl[i].len;
                data_hvl[i].len = len;
                size_t count = len * base_type.dtype_size;

                data_hvl[i].p = new char[count];
                std::memcpy(data_hvl[i].p, buf_hvl[i].p, count);

                // This assumes new references H5R_ref_t, but HL library uses old deprecated references, namely hobj_ref_t (= haddr_t)
                if (base_type.dtype_class == DatatypeClass::Reference && len > 0)
                {
                    log->info("Reference at {}, at 0: {}", i, H5Rget_type((H5R_ref_t*) buf_hvl[i].p));
                    H5R_ref_t* dest = (H5R_ref_t*) data_hvl[i].p;
                    for (size_t j = 0; j < len; ++j)
                    {
                        H5Rcopy((H5R_ref_t*) buf_hvl[i].p + j, dest + j);
                    }
                }
            }
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
        auto log = get_logger();
        // This assertion fails for varialbe length types that are deserialized.
        //log_assert(mem_type.equal(mem_type_), "Currently only know how to read the same datatype as written");

        if (mem_type.dtype_class == DatatypeClass::Compound)
        {
            hid_t type_id = mem_type_.id;
            log->trace("Attribute::read Compound");
            auto nmembers = H5Tget_nmembers(type_id);
            log->trace("Attribute::read Compound nmembers = {}", nmembers);

            for(int member_idx = 0; member_idx < nmembers; ++member_idx)
            {
                auto member_offset = H5Tget_member_offset(type_id, member_idx);

                H5T_class_t member_class = H5Tget_member_class(type_id, member_idx);

                size_t member_size = H5Tget_size(H5Tget_member_type(type_id, member_idx));

                if (member_class == H5T_REFERENCE)
                {
                    assert(sizeof(H5R_ref_t ) == member_size);
                    log->trace("Attribute::read Compound member_idx = {}, offset = {}, size = {}, REFERENCE, calling H5Rcopy", member_idx, member_offset, member_size);
                    H5R_ref_t* dest = (H5R_ref_t*)((char*)buf + member_offset);
                    H5R_ref_t* src = (H5R_ref_t*)(data.get() + member_offset);
                    H5Rcopy(src, dest);
                }
                else if (member_class == H5T_INTEGER || member_class == H5T_FLOAT)
                {
                    log->trace("Attribute::read Compound member_idx = {}, offset = {}, size = {}, arithmetic type, calling std::memcpy", member_idx, member_offset, member_size);
                    char* dest = (char*)buf + member_offset;
                    char* src = data.get() + member_offset;
                    std::memcpy(dest, src, member_size);
                } else
                {
                    log->trace("Attribute::read Compound member_idx = {}, offset = {}, size = {}, arithmetic type, member_class = {}", member_idx, member_offset, member_size, member_class);
                    throw MetadataError("unsupported member class in compound type");
                }
            }

        } else if (mem_type.dtype_class == DatatypeClass::VarLen)
        {
            log->warn("Attribute::read VarLen");
            log_assert(sizeof(hvl_t) == mem_type.dtype_size, "dtype_size must match sizeof(hvl_t); otherwise read will be incorrect");

            hvl_t* buf_hvl = (hvl_t*) buf;
            size_t nbytes = space.size() * sizeof(hvl_t);

            hvl_t* data_hvl = (hvl_t*) data.get();
            auto base_type = Datatype(H5Tget_super(mem_type.id));

            for (size_t i = 0; i < space.size(); ++i)
            {
                auto len = data_hvl[i].len;
                buf_hvl[i].len = len;
                size_t count = len * base_type.dtype_size;

                buf_hvl[i].p = new char[count];
                std::memcpy(buf_hvl[i].p, data_hvl[i].p, count);

                // This assumes new references H5R_ref_t, but HL library uses old deprecated references, namely hobj_ref_t (= haddr_t)
                if (base_type.dtype_class == DatatypeClass::Reference && len > 0)
                {
                    log->info("Reference at {}, at 0: {}", i, H5Rget_type((H5R_ref_t*) buf_hvl[i].p));
                    H5R_ref_t* dest = (H5R_ref_t*) buf_hvl[i].p;
                    for (size_t j = 0; j < len; ++j)
                    {
                        H5Rcopy((H5R_ref_t*) data_hvl[i].p + j, dest + j);
                    }
                }
            }
        } else
        if (!mem_type.is_var_length_string())
        {
            log->warn("Attribute::read regular");
            size_t nbytes = space.size() * mem_type.dtype_size;
            std::memcpy(buf, static_cast<void*>(data.get()), nbytes);
        } else  // var length string
        {
            log->warn("Attribute::read var length string");
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
        print_depth(depth);
        fmt::print(stderr, "---- Attribute ---\n");

        print_depth(depth);
        fmt::print(stderr, "type = {}, space = {}, data = {}, strings.size() = {}\n", type, space, fmt::ptr(data), strings.size());

        Object::print(depth);
    }
};

}
