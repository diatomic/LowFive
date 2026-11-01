if(NOT DEFINED MPI4PY_FINDER)
    message(FATAL_ERROR "MPI4PY_FINDER is required")
endif()

set(MPI4Py_INCLUDE_DIR "/stale/mpi4py/include" CACHE PATH "")
set(MPI4Py_VERSION "99.0" CACHE STRING "")
set(Python_EXECUTABLE "${CMAKE_COMMAND}")
include("${MPI4PY_FINDER}")

if(MPI4Py_FOUND)
    message(FATAL_ERROR "stale MPI4Py cache entries survived failed discovery")
endif()
