project                     (LowFive)
cmake_minimum_required      (VERSION 3.9)

option                      (wrapped_mpi    "MPI compiler wrapper requires no further MPI libs"     OFF)

# C++11
if                          (NOT DEFINED CMAKE_CXX_STANDARD)
    set                     (CMAKE_CXX_STANDARD 11)
endif                       ()

set                         (CMAKE_POSITION_INDEPENDENT_CODE ON)

# ignore variadic args warning (no other good solution)
set                         (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-varargs")

# Default to Release
if                          (NOT CMAKE_BUILD_TYPE)
    set                     (CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
    set_property            (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif                       (NOT CMAKE_BUILD_TYPE)

# MPI
if                          (NOT wrapped_mpi)
    find_package            (MPI REQUIRED)
    set                     (libraries MPI::MPI_C)
endif                       ()

# Threads
find_package                (Threads)

# DIY
option                      (build_examples "" OFF)
option                      (build_tests "" OFF)
add_subdirectory            (ext/diy)
set                         (libraries ${libraries} diy)

# fmt
option                      (FMT_INSTALL "" ON)
add_subdirectory            (ext/fmt)
set                         (libraries ${libraries} fmt::fmt)

# HDF5
if                          (HDF5_LIBRARY AND Z_LIBRARY)
  message                   ("HDF5_LIBRARY = " ${HDF5_LIBRARY})
  message                   ("Z_LIBRARY = " ${Z_LIBRARY})
  set                       (HDF5_LIBRARY ${HDF5_LIBRARY} ${Z_LIBRARY})
else                        ()
  message                   ("Looking for HDF5")
  set                       (HDF5_USE_STATIC_LIBRARIES ON)
  find_package              (HDF5 REQUIRED)
  message                   ("HDF5_C_LIBRARY_hdf5 = " ${HDF5_C_LIBRARY_hdf5})
  message                   ("HDF5_C_LIBRARY_z = " ${HDF5_C_LIBRARY_z})
  set                       (HDF5_LIBRARY ${HDF5_C_LIBRARY_hdf5} ${HDF5_C_LIBRARY_z})
endif                       ()
set                         (libraries ${libraries} ${HDF5_LIBRARY})
include_directories         (SYSTEM ${HDF5_INCLUDE_DIR})

# Include dirs
set                         (CMAKE_INCLUDE_SYSTEM_FLAG_CXX "-isystem")
include_directories         (${CMAKE_CURRENT_SOURCE_DIR}/include
                            SYSTEM ${MPI_C_INCLUDE_PATH})

# Libraries
set                         (libraries
                             ${libraries}
                             ${CMAKE_THREAD_LIBS_INIT}
                             ${CMAKE_DL_LIBS})

add_subdirectory            (src)
set                         (libraries ${libraries} lowfive)

# Testing
# NB testing must be enabled before adding tests subdirectory, otherwise no tests will be found
enable_testing              ()
include                     (CTest)
add_subdirectory            (tests)

add_subdirectory            (examples)
