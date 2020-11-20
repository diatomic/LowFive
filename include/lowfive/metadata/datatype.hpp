#pragma once

// TODO: need to get rid of the dependence on HighFive
#include <HighFive/H5DataType.hpp>

namespace LowFive
{

struct Datatype
{
    HighFive::DataTypeClass         dtype_class;
    size_t                          dtype_size;         // in bits
    hid_t                           dtype_id;
};

}
