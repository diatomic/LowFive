#pragma once

#include <fmt/format.h>

namespace LowFive
{

enum class      DatatypeClass;
DatatypeClass   convert_type_class(const H5T_class_t& tclass);
std::string     type_class_string(DatatypeClass tclass);

struct Datatype: Hid
{
    DatatypeClass           dtype_class;
    size_t                  dtype_size;         // in bytes

            Datatype(hid_t dtype_id_ = 0, bool owned = false):
                Hid(dtype_id_, owned)
    {
        if (id == 0) return;

        dtype_class    = convert_type_class(H5Tget_class(id));
        dtype_size     = H5Tget_size(id);
    }

    hid_t   copy() const
    {
        return H5Tcopy(id);
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
