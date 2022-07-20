#pragma once

#include <array>
#include <string>
#include <stdexcept>
#include <typeindex>

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

}
}
