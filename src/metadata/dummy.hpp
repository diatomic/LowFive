#pragma once

#include "object.hpp"

namespace LowFive
{

// TODO: this is no longer necessary, now that we replicate metadata;
//       make sure everything works and get rid of this

struct DummyObject: public Object
{
    DummyObject(ObjectType   type_,
                 std::string  name_):
        Object(type_, name_)
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

