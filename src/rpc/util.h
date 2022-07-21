#pragma once

#include <array>
#include <string>
#include <stdexcept>
#include <typeindex>

#include <functional>

#include <stdlib.h>
#include <unistd.h>

namespace LowFive
{
namespace rpc
{

namespace detail
{
// Helpers to expand a tuple into parameters for a function call, from http://stackoverflow.com/a/7858971/44738

template<int...>
struct seq {};

template<int N, int... S>
struct gens: gens<N-1, N-1, S...> {};

template<int... S>
struct gens<0, S...>
{
  typedef seq<S...> type;
};

}

inline size_t hash_combine(std::size_t seed, size_t v)
{
    return seed ^ (v + 0x9e3779b9 + (seed<<6) + (seed>>2));
}

template<class C>
size_t hash_class() { return std::type_index(typeid(C)).hash_code(); }


// From: https://stackoverflow.com/a/24068396/44738

template<typename T>
struct memfun_type
{
    using type = void;
};

template<typename Ret, typename Class, typename... Args>
struct memfun_type<Ret(Class::*)(Args...) const>
{
    using type = std::function<Ret(Args...)>;
};

// function from lambda
template<typename F>
typename memfun_type<decltype(&F::operator())>::type
to_function(F const &func)
{
    return func;
}

// function pointer
template<typename R, typename... Args>
std::function<R(Args...)>
to_function(R(*f)(Args...))
{
    return f;
}

// function pointer to member
template<typename C, typename R, typename... Args>
std::function<R(C*, Args...)>
to_function(R(C::*f)(Args...))
{
    return f;
}

}
}
