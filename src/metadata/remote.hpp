#pragma once

#include <memory>

#include "hid.hpp"
#include "object.hpp"
#include "file.hpp"
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

struct RemoteFile: public File, public RemoteObject
{
    MPI_Comm comm = MPI_COMM_NULL;

    RemoteFile(DistMetadataVOL& vol, const File& f, rpc::client::object&& obj, std::unique_ptr<Query>&& q);
    ~RemoteFile();

    DistMetadataVOL& vol;

    std::unique_ptr<Query> query_;
};

struct RemoteDataset : public Dataset, public RemoteObject
{
    using Decomposer = IndexQuery::Decomposer;
    using Bounds     = IndexQuery::Bounds;

    int                             dim;
    Decomposer                      decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy to be overwritten
    std::pair<std::string, std::string> remote_path;

            RemoteDataset(std::string name, rpc::client::object&& obj,
                          std::pair<std::string, std::string> remote_path_):
                Dataset(name, 0, 0, Ownership::lowfive, 0, 0, false, true),
                RemoteObject(std::move(obj)),
                remote_path(std::move(remote_path_))
    {
        auto log = get_logger();

        log->trace("Opened object: {} (own = {})", obj.id_, obj.own_);
        std::tie(dim, type, space, decomposer) = obj.call<std::tuple<int, Datatype, Dataspace, Decomposer>>("metadata");
    }

    void                query(const Datatype&                      mem_type,
                              const Dataspace&                     file_space,
                              const Dataspace&                     mem_space,
                              void*                                buf);
};

}

LowFive::Query*
LowFive::RemoteObject::
query(Object* o)
{
    if (!o)
        return nullptr;
    auto* file = dynamic_cast<RemoteFile*>(o->find_root());
    return file ? file->query_.get() : nullptr;
}
