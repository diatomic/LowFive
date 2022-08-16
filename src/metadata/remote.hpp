#pragma once

#include <memory>

#include "object.hpp"
#include "datatype.hpp"
#include "dataspace.hpp"

#include "../query.hpp"

namespace LowFive
{

struct RemoteObject: public Object
{
    RemoteObject(ObjectType   type_,
                 std::string  name_,
                 rpc::client::object&& obj_):
        Object(type_, name_), obj(obj_)
    {}

    inline Query*  query();

    rpc::client::object obj;
};

struct RemoteFile : public RemoteObject
{
    RemoteFile(std::string filename_, rpc::client::object&& obj, std::unique_ptr<Query>&& q):
        RemoteObject(ObjectType::File, filename_, std::move(obj)), query_(std::move(q))
    {}

    std::unique_ptr<Query> query_;
};

struct RemoteGroup : public RemoteObject
{
    RemoteGroup(std::string name, rpc::client::object&& obj):
        RemoteObject(ObjectType::Group, name, std::move(obj))
    {}
};


struct RemoteDataset : public RemoteObject
{
    using Decomposer = IndexQuery::Decomposer;
    using Bounds     = IndexQuery::Bounds;

    int                             dim;
    Datatype                        type;
    Dataspace                       space;
    Decomposer                      decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy to be overwritten

            RemoteDataset(std::string name, rpc::client::object&& obj):
                RemoteObject(ObjectType::Dataset, name, std::move(obj))
    {
        auto log = get_logger();

        log->trace("Opened object: {} (own = {})", obj.id_, obj.own_);
        std::tie(dim, type, space, decomposer) = obj.call<std::tuple<int, Datatype, Dataspace, Decomposer>>("metadata");
    }

    void                query(const Dataspace&                     file_space,      // input: query in terms of file space
                              const Dataspace&                     mem_space,       // ouput: memory space of resulting data
                              void*                                buf);            // output: resulting data, allocated by caller
};

// TODO: RemoteAttribute

}

LowFive::Query*
LowFive::RemoteObject::
query()
{
    Object* cur = this;
    while (!dynamic_cast<RemoteFile*>(cur))
        cur = cur->parent;
    return dynamic_cast<RemoteFile*>(cur)->query_.get();
}
