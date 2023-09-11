#pragma once

#include "../log-private.hpp"

namespace LowFive
{

struct CommittedDatatype: public Object
{
    CommittedDatatype(std::string name, hid_t type_id):
        Object(ObjectType::CommittedDatatype, name)
    {
        size_t nalloc = 0;
        H5Tencode(type_id, NULL, &nalloc);                  // first time sets nalloc to the required size
        data.resize(nalloc);
        H5Tencode(type_id, data.data(), &nalloc);           // second time actually encodes the data
    }

    std::vector<char> data;
};

}
