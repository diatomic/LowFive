#pragma once

#include "object.hpp"

namespace LowFive
{

struct RemoteObject: public Object
{
    RemoteObject(ObjectType   type_,
                 std::string  name_):
        Object(type_, name_)
    {}

    void* query = nullptr;
};

struct RemoteFile : public RemoteObject
{
    RemoteFile(std::string filename_):
        RemoteObject(ObjectType::File, filename_)
    {}
};

struct RemoteGroup : public RemoteObject
{
    RemoteGroup(std::string name):
        RemoteObject(ObjectType::Group, name)
    {}
};


struct RemoteDataset : public RemoteObject
{
    Datatype                        type;
    Dataspace                       space;

    RemoteDataset(std::string name):
        RemoteObject(ObjectType::Dataset, name)
    {}
};

// TODO: RemoteAttribute

}
