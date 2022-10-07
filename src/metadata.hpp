#pragma once

#include <vector>
#include <string>
#include <map>

// This is required to format enum classes with fmt as they're not implicitly
// convertible to int.
// This operator has to be declared before including format.h and ostream.h.
// We keep clang format disabled to avoid reordering and breaking things
// https://github.com/fmtlib/fmt/issues/391
#include <iostream>
#include <type_traits>
template<typename T, typename std::enable_if<std::is_enum<T>::value, int>::type = 0>
std::ostream& operator<<(std::ostream& os, const T& value)
{
  return os << static_cast<int>(value);
}

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <hdf5.h>

#include "metadata/error.hpp"

#include "metadata/hid.hpp"
#include "metadata/objecttype.hpp"
#include "metadata/dataspace.hpp"
#include "metadata/datatype.hpp"
#include "metadata/object.hpp"
#include "metadata/attribute.hpp"
#include "metadata/group.hpp"
#include "metadata/nameddtype.hpp"
#include "metadata/dataset.hpp"
#include "metadata/file.hpp"
#include "metadata/link.hpp"

#include "metadata/dummy.hpp"

#include "metadata/save-h5.hpp"
