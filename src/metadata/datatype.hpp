#pragma once

#include <algorithm>
#include <fmt/format.h>
#include "hid.hpp"
#include <cstring>
#include <vector>

//#include <hdf5.h>
#include "../log-private.hpp"

namespace LowFive
{

enum class      DatatypeClass;
DatatypeClass   convert_type_class(const H5T_class_t& tclass);
std::string     type_class_string(DatatypeClass tclass);

struct Datatype: Hid
{
    DatatypeClass           dtype_class;
    size_t                  dtype_size {0};         // in bytes

    // Borrowed datatypes are copied; newly created handles are adopted.
            Datatype(hid_t dtype_id_ = 0, bool owned = false):
                Hid(dtype_id_ && !owned ? H5Tcopy(dtype_id_) : dtype_id_, true)
    {
        if (id == 0) return;

        dtype_class    = convert_type_class(H5Tget_class(id));
        dtype_size     = H5Tget_size(id);
    }

    bool is_var_length_string() const
    {
        return id != 0 && H5Iis_valid(id) > 0 && H5Tis_variable_str(id) > 0;
    }

    bool is_modern_reference() const
    {
        return id != 0 && H5Iis_valid(id) > 0 &&
               H5Tget_class(id) == H5T_REFERENCE &&
               H5Tget_size(id) == sizeof(H5R_ref_t);
    }

    bool owns_indirect_values() const
    {
        if (id == 0 || H5Iis_valid(id) <= 0) return false;
        const auto cls = H5Tget_class(id);
        if (cls == H5T_STRING) return H5Tis_variable_str(id) > 0;
        if (cls == H5T_VLEN) return true;
        if (cls == H5T_REFERENCE) return is_modern_reference();
        if (cls == H5T_ARRAY)
        {
            Datatype base(H5Tget_super(id), true);
            return base.owns_indirect_values();
        }
        if (cls == H5T_COMPOUND)
        {
            for (int member = 0; member < H5Tget_nmembers(id); ++member)
            {
                Datatype member_type(H5Tget_member_type(id, member), true);
                if (member_type.owns_indirect_values()) return true;
            }
        }
        return false;
    }

    template<class Visitor>
    void visit_references(size_t count, void* values, Visitor&& visit) const
    {
        if (!values || id == 0 || H5Iis_valid(id) <= 0) return;
        const auto cls = H5Tget_class(id);
        for (size_t i = 0; i < count; ++i)
        {
            char* value = static_cast<char*>(values) + i * dtype_size;
            if (is_modern_reference())
                visit(reinterpret_cast<H5R_ref_t*>(value));
            else if (cls == H5T_VLEN)
            {
                Datatype base(H5Tget_super(id), true);
                auto& array = *reinterpret_cast<hvl_t*>(value);
                base.visit_references(array.len, array.p, visit);
            }
            else if (cls == H5T_COMPOUND)
            {
                for (int member = 0; member < H5Tget_nmembers(id); ++member)
                {
                    Datatype member_type(H5Tget_member_type(id, member), true);
                    member_type.visit_references(
                        1, value + H5Tget_member_offset(id, member), visit);
                }
            }
            else if (cls == H5T_ARRAY)
            {
                Datatype base(H5Tget_super(id), true);
                base.visit_references(dtype_size / base.dtype_size, value, visit);
            }
        }
    }

    bool equal(const Datatype& other) const
    {
        return H5Tequal(id, other.id) > 0;
    }

    hid_t   copy() const
    {
        return H5Tcopy(id);
    }

    // Copy pointer-bearing values independently of the caller's allocations.
    void copy_values(size_t count, const void* source, void* target) const
    {
        if (!count || source == target) return;
        const size_t bytes = count * dtype_size;
        std::vector<char> overlapping;
        const auto source_begin = reinterpret_cast<std::uintptr_t>(source);
        const auto source_end = source_begin + bytes;
        const auto target_begin = reinterpret_cast<std::uintptr_t>(target);
        const auto target_end = target_begin + bytes;
        if (source_begin < target_end && target_begin < source_end)
        {
            const auto* input = static_cast<const char*>(source);
            overlapping.assign(input, input + bytes);
            source = overlapping.data();
        }
        std::memcpy(target, source, bytes);
        const auto cls = H5Tget_class(id);
        const bool variable_string = cls == H5T_STRING && H5Tis_variable_str(id) > 0;
        const bool modern_reference = is_modern_reference();
        if (!variable_string && !modern_reference && cls != H5T_VLEN && cls != H5T_COMPOUND && cls != H5T_ARRAY)
            return;
        for (size_t i = 0; i < count; ++i)
        {
            const char* from = static_cast<const char*>(source) + i * dtype_size;
            char* to = static_cast<char*>(target) + i * dtype_size;
            if (variable_string)
            {
                const char* value = *reinterpret_cast<const char* const*>(from);
                char*& result = *reinterpret_cast<char**>(to);
                result = nullptr;
                if (value)
                {
                    result = static_cast<char*>(H5allocate_memory(std::strlen(value) + 1, false));
                    std::strcpy(result, value);
                }
            }
            else if (cls == H5T_VLEN)
            {
                Datatype base(H5Tget_super(id), true);
                const auto& value = *reinterpret_cast<const hvl_t*>(from);
                auto& result = *reinterpret_cast<hvl_t*>(to);
                result.p = value.len ? H5allocate_memory(value.len * base.dtype_size, true) : nullptr;
                base.copy_values(value.len, value.p, result.p);
            }
            else if (cls == H5T_COMPOUND)
            {
                for (int member = 0; member < H5Tget_nmembers(id); ++member)
                {
                    Datatype member_type(H5Tget_member_type(id, member), true);
                    size_t offset = H5Tget_member_offset(id, member);
                    member_type.copy_values(1, from + offset, to + offset);
                }
            }
            else if (cls == H5T_ARRAY)
            {
                Datatype base(H5Tget_super(id), true);
                base.copy_values(dtype_size / base.dtype_size, from, to);
            }
            else if (modern_reference)
            {
                auto* ref = reinterpret_cast<const H5R_ref_t*>(from);
                auto* result = reinterpret_cast<H5R_ref_t*>(to);
                std::memset(result, 0, sizeof(*result));
                const auto* bytes =
                    reinterpret_cast<const unsigned char*>(ref);
                const bool empty = std::all_of(
                    bytes, bytes + sizeof(*ref),
                    [](unsigned char value) { return value == 0; });
                if (!empty && H5Rcopy(ref, result) < 0)
                    throw MetadataError("Could not copy HDF5 reference");
            }
        }
    }

    void destroy_values(size_t count, void* values) const
    {
        if (!values || H5Iis_valid(id) <= 0) return;
        const auto cls = H5Tget_class(id);
        const bool variable_string = cls == H5T_STRING && H5Tis_variable_str(id) > 0;
        const bool modern_reference = is_modern_reference();
        if (!variable_string && !modern_reference && cls != H5T_VLEN && cls != H5T_COMPOUND && cls != H5T_ARRAY)
            return;
        for (size_t i = 0; i < count; ++i)
        {
            char* value = static_cast<char*>(values) + i * dtype_size;
            if (variable_string)
                H5free_memory(*reinterpret_cast<char**>(value));
            else if (cls == H5T_VLEN)
            {
                Datatype base(H5Tget_super(id), true);
                auto& array = *reinterpret_cast<hvl_t*>(value);
                base.destroy_values(array.len, array.p);
                H5free_memory(array.p);
            }
            else if (cls == H5T_COMPOUND)
            {
                for (int member = 0; member < H5Tget_nmembers(id); ++member)
                {
                    Datatype member_type(H5Tget_member_type(id, member), true);
                    member_type.destroy_values(1, value + H5Tget_member_offset(id, member));
                }
            }
            else if (cls == H5T_ARRAY)
            {
                Datatype base(H5Tget_super(id), true);
                base.destroy_values(dtype_size / base.dtype_size, value);
            }
            else if (modern_reference)
            {
                auto* ref = reinterpret_cast<H5R_ref_t*>(value);
                if (H5Rget_type(ref) != H5R_BADTYPE)
                    H5Rdestroy(ref);
            }
        }
    }

    // Source and destination are packed arrays; selection mapping belongs to the caller.
    void convert(const Datatype& destination, size_t count, const void* source, void* target) const
    {
        if (!count) return;
        if (equal(destination))
        {
            copy_values(count, source, target);
            return;
        }
        H5T_cdata_t* conversion = nullptr;
        if (!H5Tfind(id, destination.id, &conversion))
            throw MetadataError("Could not find HDF5 datatype conversion");
        std::vector<char> background;
        if (conversion->need_bkg != H5T_BKG_NO)
            background.resize(count * destination.dtype_size, 0);
        std::vector<char> buffer;
        void* values = target;
        if (destination.dtype_size < dtype_size)
        {
            buffer.resize(count * dtype_size);
            values = buffer.data();
        }
        std::memcpy(values, source, count * dtype_size);
        if (H5Tconvert(id, destination.id, count, values, background.empty() ? nullptr : background.data(), H5P_DEFAULT) < 0)
            throw MetadataError("Could not convert HDF5 datatype");
        if (values != target)
            std::memcpy(target, values, count * destination.dtype_size);
    }

    std::string class_string() const
    {
        return type_class_string(dtype_class);
    }

    friend std::ostream& operator<<(std::ostream& out, const Datatype& dt)
    {
        fmt::print(out, "{}{}", dt.class_string(), dt.dtype_size);
        return out;
    }
};


// borrowed from HighFive

enum class DatatypeClass {
    Time,
    Integer,
    Float,
    String,
    BitField,
    Opaque,
    Compound,
    Reference,
    Enum,
    VarLen,
    Array,
    Invalid
};

inline DatatypeClass convert_type_class(const H5T_class_t& tclass) {
    switch(tclass) {
        case H5T_TIME:
            return DatatypeClass::Time;
        case H5T_INTEGER:
            return DatatypeClass::Integer;
        case H5T_FLOAT:
            return DatatypeClass::Float;
        case H5T_STRING:
            return DatatypeClass::String;
        case H5T_BITFIELD:
            return DatatypeClass::BitField;
        case H5T_OPAQUE:
            return DatatypeClass::Opaque;
        case H5T_COMPOUND:
            return DatatypeClass::Compound;
        case H5T_REFERENCE:
            return DatatypeClass::Reference;
        case H5T_ENUM:
            return DatatypeClass::Enum;
        case H5T_VLEN:
            return DatatypeClass::VarLen;
        case H5T_ARRAY:
            return DatatypeClass::Array;
        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            return DatatypeClass::Invalid;
    }
}

inline std::string type_class_string(DatatypeClass tclass) {
    switch(tclass) {
        case DatatypeClass::Time:
            return "Time";
        case DatatypeClass::Integer:
            return "Integer";
        case DatatypeClass::Float:
            return "Float";
        case DatatypeClass::String:
            return "String";
        case DatatypeClass::BitField:
            return "BitField";
        case DatatypeClass::Opaque:
            return "Opaque";
        case DatatypeClass::Compound:
            return "Compound";
        case DatatypeClass::Reference:
            return "Reference";
        case DatatypeClass::Enum:
            return "Enum";
        case DatatypeClass::VarLen:
            return "Varlen";
        case DatatypeClass::Array:
            return "Array";
        default:
            return "(Invalid)";
    }
}

}
