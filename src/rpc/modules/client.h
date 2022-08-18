#pragma once

#include <string>
#include <functional>
#include <tuple>
#include <typeindex>
#include <map>
#include <vector>

#include "../util.h"

#include "../../log-private.hpp"

namespace LowFive
{

namespace rpc
{

struct client::module
{
    using NameArgs = std::tuple<std::string, size_t>;

    struct class_proxy;
    template<class... Args> struct hash_arguments;      // for call
    template<class... Args> struct hash_parameters;     // for function

    template<class F>
    void            function(std::string name, F f)
    {
        function(name, to_function(f));
    }

    template<class R, class... Args>
    void        function(std::string name, R (*f)(Args...))
    {
        function(name, to_function(f));
    }

    template<class R, class... Args>
    void        function(std::string name, std::function<R(Args...)>)
    {
        size_t hash = hash_parameters<Args...>()();
        //std::cout << "Recording function " << name << " with parameter hash " << hash << std::endl;
        auto name_args = std::make_tuple(name, hash);
        size_t fn = functions_.size();
        //std::cout << "  function id: " << fn << std::endl;
        functions_[name_args] = fn;
    }

    template<class C>
    class_proxy&    class_(std::string name);

    template<class R, class... Args>
    size_t          find(std::string name, Args... args) const
    {
        size_t hash = hash_arguments<Args...>()(args...);
        //std::cout << "Looking for function " << name << " with argument hash " << hash << std::endl;
        auto name_args = std::make_tuple(name, hash);
        auto it = functions_.find(name_args);
        if (it == functions_.end())
            throw std::runtime_error("Function " + name + " not found");

        //std::cout << "  found function id: " << it->second << std::endl;

        return it->second;
    }

    size_t          find_class(std::string name) const
    {
        auto it = classes_.find(name);
        if (it == classes_.end())
            throw std::runtime_error("Class " + name + " not found");
        size_t id = it->second;
        return id;
    }

    template<class... Args>
    std::tuple<size_t,size_t>       find_class_constructor(std::string name, Args... args) const;

    const class_proxy&              proxy(size_t id) const                         { return class_proxies_[id]; }

    std::map<NameArgs, size_t>      functions_;
    std::map<std::string, size_t>   classes_;
    std::vector<class_proxy>        class_proxies_;
};

/**
 * Class proxy represents a class.
 */
struct client::module::class_proxy
{
                    class_proxy(size_t class_hash):
                        class_hash_(class_hash)             {}

    template<class... Args>
    class_proxy&    constructor()
    {
        auto args = client::module::hash_parameters<Args...>()();
        size_t fn = constructors_.size();
        constructors_[args] = fn;
        return *this;
    }

    template<class F>
    class_proxy&    function(std::string name, F f)
    {
        return function(name, to_function(f));
    }

    template<class R, class T, class... Args>
    class_proxy&    function(std::string name, R (T::*f)(Args...))
    {
        return function(name, to_function(f));
    }

    template<class R, class T, class... Args>
    class_proxy&    function(std::string name, std::function<R(T*, Args...)>)
    {
        size_t hash = client::module::hash_parameters<Args...>()();
        //std::cout << "Recording member function " << name << " with parameter hash " << hash << std::endl;
        auto name_args = std::make_tuple(name, hash);
        size_t fn = functions_.size();
        //std::cout << "  function id: " << fn << std::endl;
        functions_[name_args] = fn;
        return *this;
    }

    template<class R, class... Args>
    size_t          find(std::string name, Args... args) const
    {
        size_t hash = client::module::hash_arguments<Args...>()(args...);
        //std::cout << "Looking for member function " << name << " with argument hash " << hash << std::endl;
        auto name_args = std::make_tuple(name, hash);
        auto it = functions_.find(name_args);
        if (it == functions_.end())
            throw std::runtime_error("Member function " + name + " not found");

        //std::cout << "  found function id: " << it->second << std::endl;

        return it->second;
    }

    template<class... Args>
    size_t          find_constructor(Args... args) const
    {
        size_t args_hash = client::module::hash_arguments<Args...>()(args...);
        auto it = constructors_.find(args_hash);
        if (it == constructors_.end())
            throw std::runtime_error("Constructor not found");

        return it->second;
    }

    size_t          hash() const                            { return class_hash_; }

    size_t                          class_hash_;
    std::map<NameArgs, size_t>      functions_;
    std::map<size_t, size_t>        constructors_;
};

template<class C>
client::module::class_proxy&
client::module::
class_(std::string name)
{
    size_t c = classes_.size();
    classes_[name] = c;
    size_t class_hash = std::type_index(typeid(C)).hash_code();
    class_proxies_.push_back(class_proxy(class_hash));
    return class_proxies_.back();
}

template<class... Args>
std::tuple<size_t,size_t>
client::module::
find_class_constructor(std::string name, Args... args) const
{
    size_t class_id = find_class(name);
    size_t constructor_id = class_proxies_[class_id].find_constructor(args...);
    return std::make_tuple(class_id, constructor_id);
}

/**
 * object holds an id of the remote object and a reference to its class_proxy (which lets us recover its class hash_code)
 */

struct client::object
{
    using class_proxy = client::module::class_proxy;

                        object(int rank, size_t id, const class_proxy& cp, client* self, bool own):
                            rank_(rank), id_(id), cp_(cp), self_(self), own_(own)
    {
        self_->ref_count(rank_, id_)++;
    }

                        object(const object& o):
                            rank_(o.rank_), id_(o.id_), cp_(o.cp_), self_(o.self_), own_(o.own_)
                                                                    { self_->ref_count(rank_, id_)++; }
                        object(object&& o):
                            rank_(o.rank_), id_(o.id_), cp_(o.cp_), self_(o.self_), own_(o.own_)
                                                                    { o.self_ = nullptr; } // leave o in invalid state

    object&             operator=(const object&)    =delete;
    object&             operator=(object&&)         =delete;

    template<class R, class... Args>
    R                   call(std::string name, Args... args)        { return self_->call_mem_fn<R,Args...>(rank_, id_, cp_.find<R, Args...>(name, args...), args...); }

    size_t              hash() const                                { return cp_.hash(); }
    size_t              id() const                                  { return id_; }

    void                destroy()
    {
        if (!self_)      // invalid state
            return;

        auto log = get_logger();
        log->trace("~object: own = {}, this = {}", own_, fmt::ptr(this));

        self_->ref_count(rank_, id_)--;
        if (own_ && self_->ref_count(rank_, id_) == 0)
            self_->destroy(rank_, id_);

        self_ = nullptr;
    }

                        ~object()                                   { destroy(); }

    int                 rank_;
    size_t              id_;
    const class_proxy&  cp_;
    client*             self_;
    bool                own_;
};

/**
 * hash_arguments<Args...> + hash_parameters<Args...>
 *
 * We don't use type_index(std::tuple<Args...>) directly because hash_parameters
 * has to deal with objects (on the call side), where we use their hash value directly.
 * Therefore, we use a custom implementation.
 */

template<class T, class... Args>
struct client::module::hash_parameters<T, Args...>
{
    size_t      operator()() const
    {
        return hash_combine(hash_parameters<Args...>()(), std::type_index(typeid(T)).hash_code());
    }
};

template<class T, class... Args>
struct client::module::hash_parameters<T&, Args...>
{
    size_t      operator()() const
    {
        size_t seed = hash_parameters<Args...>()();
        seed = hash_combine(seed, 1);
        seed = hash_combine(seed, std::type_index(typeid(T)).hash_code());
        return seed;
    }
};

template<class T, class... Args>
struct client::module::hash_parameters<T*, Args...>
{
    size_t      operator()() const
    {
        size_t seed = hash_parameters<Args...>()();
        seed = hash_combine(seed, 2);
        seed = hash_combine(seed, std::type_index(typeid(T)).hash_code());
        return seed;
    }
};

template<>
struct client::module::hash_parameters<>
{
    size_t      operator()() const      { return 0; }
};


template<class T, class... Args>
struct client::module::hash_arguments<T, Args...>
{
    size_t      operator()(T x, Args... args) const
    {
        return hash_combine(hash_arguments<Args...>()(args...), std::type_index(typeid(T)).hash_code());
    }
};

template<class... Args>
struct client::module::hash_arguments<client::object, Args...>
{
    size_t      operator()(client::object& o, Args... args) const
    {
        size_t seed = hash_arguments<Args...>()(args...);
        seed = hash_combine(seed, 1);
        seed = hash_combine(seed, o.hash());
        return seed;
    }
};

template<class... Args>
struct client::module::hash_arguments<client::object*, Args...>
{
    size_t      operator()(client::object* o, Args... args) const
    {
        size_t seed = hash_arguments<Args...>()(args...);
        seed = hash_combine(seed, 2);
        seed = hash_combine(seed, o->hash());
        return seed;
    }
};

template<>
struct client::module::hash_arguments<>
{
    size_t      operator()() const      { return 0; }
};

}   // namespace rpc
}   // namespace LowFive
