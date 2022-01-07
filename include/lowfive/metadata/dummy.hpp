#pragma once

#include "object.hpp"

namespace LowFive
{

struct DummyObject: public Object
{
    DummyObject(ObjectType   type_,
                 std::string  name_,
                 bool         remove_path = false):     // by default, remote objects don't strip paths from their names
        Object(type_, name_, remove_path)
    {}
};

struct DummyFile : public DummyObject
{
    DummyFile(std::string filename_):
        DummyObject(ObjectType::File, filename_)
    {}
};

struct DummyGroup : public DummyObject
{
    DummyGroup(std::string name):
        DummyObject(ObjectType::Group, name)
    {}
};


struct DummyDataset : public DummyObject
{
    Datatype                        type;
    Dataspace                       space;

    DummyDataset(std::string name):
        DummyObject(ObjectType::Dataset, name)
    {}
};

// TODO: DummyAttribute

}
