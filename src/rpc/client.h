#pragma once

#include <map>

#include <mpi.h>
#include <diy/mpi.hpp>

#include <lowfive/metadata/serialization.hpp>

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
    template<class... Args> struct save_impl;
    template<class R>       struct load_impl;

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
        const module&               m_;
        diy::mpi::communicator      comm_;

        int                         default_target_;

        std::map<std::tuple<int,size_t>, size_t>    ref_count_;

};

}   // namespace rpc
}   // namespace LowFive

#include "modules/client.h"

// Re-open the namespace for specializations to work around a GCC bug;
// see http://stackoverflow.com/a/25594741
namespace LowFive
{
namespace rpc
{

// load_impl exists to deal with void returns and custom handling of objects
template<class R>
struct client::load_impl
{
                            load_impl(diy::MemoryBuffer& in, int, client*, bool):
                                in_(in)                 {}

    R                       operator()() const          { R x; diy::load(in_, x); return x; }

    diy::MemoryBuffer&      in_;
};

template<>
struct client::load_impl<client::object>
{
                            load_impl(diy::MemoryBuffer& in, int target, client* self, bool own):
                                in_(in), target_(target), self_(self)
                                                        {}

    object                  operator()() const
    {
        // XXX: without this log->trace(), we sometimes get a completely weird
        //      behavior, where own_ = true, despite it actually being false.
        //      I have not been able to figure out why this happens.
        auto log = get_logger();
        log->trace("In load_impl, returning object: own = {}", own_);

        size_t obj_id, cls_id;
        diy::load(in_, cls_id);
        diy::load(in_, obj_id);

        return object(target_, obj_id, self_->m_.proxy(cls_id), self_, own_);
    }

    diy::MemoryBuffer&      in_;
    int                     target_;
    client*                 self_;
    bool                    own_;
};

template<>
struct client::load_impl<void>
{
                            load_impl(diy::MemoryBuffer&, int, client*, bool)   {}

    void                    operator()() const          {}
};

}
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

    save_impl<Args...> s(out);
    s(args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load_impl<R>(in, target, this, false)();
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

    save_impl<Args...> s(out);
    s(args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load_impl<R>(in, target, this, false)();
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

    save_impl<Args...> s(out);
    s(args...);

    comm_.send(target, tags::consumer, out.buffer);
    comm_.recv(target, tags::producer, in.buffer);

    return load_impl<object>(in, target, this, true)();     // true indicates that we own this object (and will automatically delete it, when all references go out of scope)
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

// Re-open the namespace for specializations to work around a GCC bug;
// see http://stackoverflow.com/a/25594741
namespace LowFive
{
namespace rpc
{

template<class T, class... Args>
struct client::save_impl<T, Args...>
{
                            save_impl(diy::MemoryBuffer& out):
                                out_(out)       {}

    void                    operator()(T x, Args... args)               { diy::save(out_, x); save_impl<Args...> s(out_); s(args...); }

    diy::MemoryBuffer&      out_;
};

template<class... Args>
struct client::save_impl<rpc::client::object, Args...>
{
                            save_impl(diy::MemoryBuffer& out):
                                out_(out)       {}

    void                    operator()(object& o, Args... args) const   { diy::save(out_, o.id()); save_impl<Args...> s(out_); s(args...); }

    diy::MemoryBuffer&      out_;
};

template<class... Args>
struct client::save_impl<rpc::client::object*, Args...>
{
                            save_impl(diy::MemoryBuffer& out):
                                out_(out)       {}

    void                    operator()(object* o, Args... args) const   { diy::save(out_, o->id()); save_impl<Args...> s(out_); s(args...); }

    diy::MemoryBuffer&      out_;
};

template<>
struct client::save_impl<>
{
                            save_impl(diy::MemoryBuffer&)   {}

    void                    operator()() const          {}
};

}   // namespace rpc
}   // namespace LowFive
