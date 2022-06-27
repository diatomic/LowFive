#pragma once

#include <tuple>
#include <vector>

#include "exchange.h"
#include "operations.h"

namespace LowFive
{

namespace rpc
{

struct server: public exchange
{
    struct module;
    struct object;
    template<class... Args> struct receive_impl;

                    server(const module& m, stream& in, stream& out):
                        exchange(in,out), m_(m)     {}

    template<class... Args>
    std::tuple<Args...>
                    receive()                       { return receive_impl<Args...>(this)(); }

    template<class T>
    void            send(const T& x)                { exchange::send(x); exchange::flush(); }       // here flush is built in since the server only sends back one answer at a time

    inline void     listen();

    private:
        const module&           m_;
        bool                    done_ = false;
        std::vector<void*>      objects_;
        std::vector<size_t>     object_classes_;
};

template<class T, class... Args>
struct server::receive_impl<T, Args...>
{
                            receive_impl(server* self):
                                self_(self)         {}

    std::tuple<T, Args...>  operator()() const
    {
        T x;
        self_->recv(x);
        return std::tuple_cat(std::make_tuple(x), receive_impl<Args...>(self_)());
    }

    server* self_;
};

// T*
template<class T, class... Args>
struct server::receive_impl<T*, Args...>
{
                            receive_impl(server* self):
                                self_(self)         {}

    std::tuple<T*, Args...> operator()() const
    {
        size_t obj_id;
        self_->recv(obj_id);
        return std::tuple_cat(std::make_tuple(static_cast<T*>(self_->objects_[obj_id])), receive_impl<Args...>(self_)());
    }

    server* self_;
};

// T&
template<class T, class... Args>
struct server::receive_impl<T&, Args...>
{
                            receive_impl(server* self):
                                self_(self)         {}

    std::tuple<T&, Args...> operator()() const
    {
        size_t obj_id;
        self_->recv(obj_id);
        T& x = *static_cast<T*>(self_->objects_[obj_id]);
        return std::tuple_cat(std::tuple<T&>(x), receive_impl<Args...>(self_)());
    }

    server* self_;
};

template<>
struct server::receive_impl<>
{
                            receive_impl(server* self):
                                self_(self)         {}

    std::tuple<>            operator()() const      { return std::tuple<>(); }

    server* self_;
};

}   // namespace rpc
}   // namespace LowFive

#include "modules/server.h"

void
LowFive::rpc::
server::
listen()
{
    while(!done_)
    {
        ops::Operation op;
        recv(op);

        //std::cout << "Received op: " << op << std::endl;

        if (op == ops::function)
        {
            size_t id;
            recv(id);
            //std::cout << "Function call " << id << std::endl;
            m_.call(id, this);
        } else if (op == ops::mem_fn)
        {
            size_t obj_id;
            recv(obj_id);
            size_t class_id = object_classes_[obj_id];
            auto& cp = m_.proxy(class_id);
            size_t fn_id;
            recv(fn_id);

            //std::cout << "Member function call in object " << obj_id << ", function " << fn_id << std::endl;

            cp.call(fn_id, obj_id, this);
        } else if (op == ops::create)
        {
            size_t class_id, constructor_id;
            recv(class_id);
            recv(constructor_id);
            objects_.push_back(m_.create(class_id, constructor_id, this));
            object_classes_.push_back(class_id);
            size_t obj_id = objects_.size() - 1;
            send(obj_id);
        } else if (op == ops::destroy)
        {
            size_t obj_id;
            recv(obj_id);
            m_.proxy(obj_id).destroy(objects_[obj_id]);
            objects_[obj_id] = 0;
        } else if (op == ops::finish)
            done_ = true;
        else
            throw std::runtime_error("Uknown operation");
    }
}
