#pragma once

#include <map>

#include <mpi.h>
#include <diy/mpi.hpp>

#include "../metadata/serialization.hpp"

#include "operations.h"

// client and server rely on functions being registered in the same order to
// work across platforms

namespace LowFive
{

namespace rpc
{

struct client
{
    struct module;
    struct object;

                    client(const module& m, MPI_Comm comm, int default_target):
                        m_(m), comm_(comm), default_target_(default_target)
                                                                        {}

    template<class R, class... Args>
    R               call(std::string name, Args... args)                { return call<R, Args...>(default_target_, name, args...); }

    template<class R, class... Args>
    R               call(int target, std::string name, Args... args);

    template<class R, class... Args>
    R               call_mem_fn(size_t obj, size_t fn, Args... args)    { return call_mem_fn<R, Args...>(default_target_, obj, fn, args...); }

    template<class R, class... Args>
    R               call_mem_fn(int target, size_t obj, size_t fn, Args... args);

    template<class... Args>
    object          create(std::string name, Args... args);

    template<class... Args>
    object          create(int target, std::string name, Args... args);

    inline void     destroy(int target, size_t id);

    inline size_t&  ref_count(int target, size_t obj_id)    { return ref_count_[std::make_tuple(target, obj_id)]; }
    inline void     finish(int target) const                { diy::MemoryBuffer out; diy::save(out, ops::finish); comm_.send(target, tags::consumer, out.buffer); }

    private:
        template<class R>
        R           load(diy::MemoryBuffer& in, int target, bool own);

        template<class... Args>
        void        save(diy::MemoryBuffer& out, Args... args);

    private:
        const module&               m_;
        diy::mpi::communicator      comm_;

        int                         default_target_;

        std::map<std::tuple<int,size_t>, size_t>    ref_count_;

};

}   // namespace rpc
}   // namespace LowFive

#include "modules/client.h"

template<class R>
R
LowFive::rpc::client::
load(diy::MemoryBuffer& in, int target, bool own)
{
    if constexpr (std::is_same_v<R, object>)
    {
        size_t obj_id, cls_id;
        diy::load(in, cls_id);
        diy::load(in, obj_id);
        return object(target, obj_id, m_.proxy(cls_id), this, own);
    } else if constexpr (!std::is_void_v<R>)
    {
        R x;
        diy::load(in, x);
        return x;
    }
}

template<class... Args>
void
LowFive::rpc::client::
save(diy::MemoryBuffer& out, Args... args)
{
    ([&out](auto x)
     {
        if constexpr (std::is_same_v<decltype(x), object>)
            diy::save(out, x.id());
        else if constexpr (std::is_same_v<decltype(x), object*>)
            diy::save(out, x->id());
        else
            diy::save(out, x);
     } (args), ...);
}

template<class R, class... Args>
R
LowFive::rpc::client::
call(int target, std::string name, Args... args)
{
    // find the functions in the module
    size_t id = m_.template find<R,Args...>(name, args...);

    diy::MemoryBuffer out, in;

    // send the unique identifier + serialized arguments
    diy::save(out, ops::function);
    diy::save(out, id);

    save(out, args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load<R>(in, target, false);
}

template<class R, class... Args>
R
LowFive::rpc::client::
call_mem_fn(int target, size_t obj, size_t fn, Args... args)
{
    diy::MemoryBuffer out, in;

    // send the unique identifier + serialized arguments
    diy::save(out, ops::mem_fn);
    diy::save(out, obj);
    diy::save(out, fn);

    save(out, args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load<R>(in, target, false);
}

template<class... Args>
LowFive::rpc::client::object
LowFive::rpc::client::create(int target, std::string name, Args... args)
{
    size_t cls_id, constructor_id;
    std::tie(cls_id,constructor_id) = m_.find_class_constructor(name, args...);

    diy::MemoryBuffer out, in;

    diy::save(out, ops::create);
    diy::save(out, cls_id);
    diy::save(out, constructor_id);

    save(out, args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load<object>(in, target, true);     // true indicates that we own this object (and will automatically delete it, when all references go out of scope)
}

template<class... Args>
LowFive::rpc::client::object
LowFive::rpc::client::create(std::string name, Args... args)
{
    return create(default_target_, name, args...);
}

void
LowFive::rpc::client::destroy(int target, size_t id)
{
    diy::MemoryBuffer out;
    diy::save(out, ops::destroy);
    diy::save(out, id);

    comm_.send(target, tags::consumer, out.buffer);
}
