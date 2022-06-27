#pragma once

#include <map>

#include "exchange.h"
#include "operations.h"

// client and server rely on functions being registered in the same order to
// work across platforms

namespace LowFive
{

namespace rpc
{

struct client: public exchange
{
    struct module;
    struct object;
    template<class... Args> struct send_impl;
    template<class R>       struct recv_impl;

                    client(const module& m, in_stream& in, out_stream& out):
                        exchange(in,out), m_(m)         {}

    template<class R, class... Args>
    R               call(std::string name, Args... args);

    template<class R, class... Args>
    R               call_mem_fn(size_t obj, size_t fn, Args... args);

    template<class... Args>
    object          create(std::string name, Args... args);

    size_t&         ref_count(size_t obj_id)            { return ref_count_[obj_id]; }

                    ~client()                           { send(ops::finish); exchange::flush(); }

    private:
        const module&               m_;
        std::map<size_t, size_t>    ref_count_;

};

}   // namespace rpc
}   // namespace LowFive

#include "modules/client.h"

template<class R, class... Args>
R
LowFive::rpc::client::
call(std::string name, Args... args)
{
    // find the functions in the module
    size_t id = m_.template find<R,Args...>(name, args...);

    // send the unique identifier + serialized arguments
    send(ops::function);
    send(id);

    send_impl<Args...>(this)(args...);

    exchange::flush();

    return recv_impl<R>(this)();
}

template<class R, class... Args>
R
LowFive::rpc::client::
call_mem_fn(size_t obj, size_t fn, Args... args)
{
    // send the unique identifier + serialized arguments
    send(ops::mem_fn);
    send(obj);
    send(fn);

    send_impl<Args...>(this)(args...);

    exchange::flush();

    return recv_impl<R>(this)();
}

template<class... Args>
LowFive::rpc::client::object
LowFive::rpc::client::create(std::string name, Args... args)
{
    size_t id, constructor_id;
    std::tie(id,constructor_id) = m_.find_class_constructor(name, args...);
    send(ops::create);
    send(id);
    send(constructor_id);

    send_impl<Args...>(this)(args...);

    exchange::flush();

    size_t obj_id;
    recv(obj_id);
    return object(obj_id, m_.proxy(id), this);
}

// Re-open the namespace for specializations to work around a GCC bug;
// see http://stackoverflow.com/a/25594741
namespace LowFive
{
namespace rpc
{

template<class T, class... Args>
struct client::send_impl<T, Args...>
{
                            send_impl(client* self):
                                self_(self)     {}

    void                    operator()(T x, Args... args) const         { self_->send(x); send_impl<Args...> s(self_); s(args...); }

    client* self_;
};

template<class... Args>
struct client::send_impl<rpc::client::object, Args...>
{
                            send_impl(client* self):
                                self_(self)     {}

    void                    operator()(object& o, Args... args) const   { self_->send(o.id()); send_impl<Args...> s(self_); s(args...); }

    client* self_;
};

template<class... Args>
struct client::send_impl<rpc::client::object*, Args...>
{
                            send_impl(client* self):
                                self_(self)     {}

    void                    operator()(object* o, Args... args) const   { self_->send(o->id()); send_impl<Args...> s(self_); s(args...); }

    client* self_;
};

template<>
struct client::send_impl<>
{
                            send_impl(client* self):
                                self_(self)     {}

    void                    operator()() const              {}

    client* self_;
};

// recv_impl exists to deal with void returns
template<class R>
struct client::recv_impl
{
                            recv_impl(client* self):
                                self_(self)     {}

    R                       operator()() const          { R x; self_->recv(x); return x; }

    client* self_;
};

template<>
struct client::recv_impl<void>
{
                            recv_impl(client* self):
                                self_(self)     {}

    void                    operator()() const          {}

    client* self_;
};

}   // namespace rpc
}   // namespace LowFive
