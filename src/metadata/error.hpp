#pragma once

namespace LowFive
{

struct MetadataError: public std::runtime_error
{
    using std::runtime_error::runtime_error;
};

}
