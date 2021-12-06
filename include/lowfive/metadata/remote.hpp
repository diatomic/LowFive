#pragma once

#include "object.hpp"

namespace LowFive
{

struct RemoteObject: public Object
{
    RemoteObject(ObjectType   type_,
                 std::string  name_,
                 bool         remove_path = false):     // by default, remote objects don't strip paths from their names
        Object(type_, name_, remove_path)
    {}

    void* query = nullptr;
};

struct RemoteFile : public RemoteObject
{
    RemoteFile(std::string filename_):
        RemoteObject(ObjectType::File, filename_)
    {}
};

struct RemoteDataset : public RemoteObject
{
    RemoteDataset(std::string name):
        RemoteObject(ObjectType::Dataset, name)
    {}
};

}
