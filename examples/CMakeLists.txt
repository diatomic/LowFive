include_directories         (${CMAKE_CURRENT_SOURCE_DIR}/include)

# HighFive
include_directories         (SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../ext/HighFive/include)

# add_subdirectory            (native-passthru)
# add_subdirectory            (vol-wrapper)
# add_subdirectory            (variety)
#add_subdirectory            (ghost)
# add_subdirectory            (hdf5-experiments)
#add_subdirectory            (m-to-n)
# add_subdirectory            (prod-con)
add_subdirectory            (prod-con-multidata)
