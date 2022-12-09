#pragma once

#include <memory>

#include "object.hpp"
#include "datatype.hpp"
#include "dataspace.hpp"

#include "../query.hpp"

namespace LowFive
{

struct RemoteObject
{
    RemoteObject(rpc::client::object&& obj_):
        obj(obj_)
    {}

    static inline Query*  query(Object* o);

    rpc::client::object obj;
};

struct RemoteFile: public Object, public RemoteObject
{
    RemoteFile(std::string filename_, rpc::client::object&& obj, std::unique_ptr<Query>&& q):
        Object(ObjectType::File, filename_),
        RemoteObject(std::move(obj)), query_(std::move(q))
    {}

    ~RemoteFile()
    {
        // RemoteFile needs to delete children before query_, so their obj get destroyed
        // So it's not enough to do this via ~Object

        for (auto* child : children)
        {
            child->parent = nullptr;    // to skip remove() in child
            delete child;
        }
        children.clear();

        obj.destroy();                  // must destroy our obj before query_
    }

    std::unique_ptr<Query> query_;
};

struct RemoteDataset : public Dataset, public RemoteObject
{
    using Decomposer = IndexQuery::Decomposer;
    using Bounds     = IndexQuery::Bounds;

    int                             dim;
    Decomposer                      decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy to be overwritten

            RemoteDataset(std::string name, rpc::client::object&& obj):
                Dataset(name, 0, 0, Ownership::lowfive, 0, 0),
                RemoteObject(std::move(obj))
    {
        auto log = get_logger();

        log->trace("Opened object: {} (own = {})", obj.id_, obj.own_);
        std::tie(dim, type, space, decomposer) = obj.call<std::tuple<int, Datatype, Dataspace, Decomposer>>("metadata");
    }

    void                query(const Dataspace&                     file_space,      // input: query in terms of file space
                              const Dataspace&                     mem_space,       // ouput: memory space of resulting data
                              void*                                buf);            // output: resulting data, allocated by caller
};

}

LowFive::Query*
LowFive::RemoteObject::
query(Object* o)
{
    Object* cur = o;
    while (cur && !dynamic_cast<RemoteFile*>(cur))
        cur = cur->parent;
    if (!cur)
        return nullptr;
    return dynamic_cast<RemoteFile*>(cur)->query_.get();
}
