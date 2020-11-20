# TODO: get rid of this dependence
# HighFive
include_directories         (SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../ext/HighFive/include)

add_library             (lowfive vol-base.cpp vol-metadata.cpp)
target_link_libraries   (lowfive fmt::fmt)
