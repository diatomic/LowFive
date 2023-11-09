# HighFive
include_directories         (SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../ext/HighFive/include)

add_subdirectory            (prod-con-multidata)
add_subdirectory            (attributes)
add_subdirectory            (links)
add_subdirectory            (location)
add_subdirectory            (datatypes)
add_subdirectory            (dimension-scales)
add_subdirectory            (simple-metadata)
add_subdirectory            (groups)
