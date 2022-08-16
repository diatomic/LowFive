#pragma once

#include <fmt/ostream.h>

namespace LowFive
{

enum class ObjectType
{
    File,
    Group,
    Dataset,
    Attribute,
    NamedDtype,
    HardLink,
    SoftLink
};

}

namespace std
{
    inline std::ostream& operator<<(std::ostream& out, const LowFive::ObjectType& t)
    {
        out << (int) t;
        return out;
    }
}
