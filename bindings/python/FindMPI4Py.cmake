# Discover mpi4py through the interpreter used to build the extension.
execute_process(COMMAND "${Python_EXECUTABLE}" -c "import mpi4py; print(mpi4py.get_include())"
                RESULT_VARIABLE _MPI4Py_INCLUDE_RESULT
                OUTPUT_VARIABLE MPI4Py_INCLUDE_DIR
                OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
execute_process(COMMAND "${Python_EXECUTABLE}" -c "import mpi4py; print(mpi4py.__version__)"
                RESULT_VARIABLE _MPI4Py_VERSION_RESULT
                OUTPUT_VARIABLE MPI4Py_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
if(_MPI4Py_INCLUDE_RESULT OR NOT EXISTS "${MPI4Py_INCLUDE_DIR}/mpi4py/mpi4py.h")
    unset(MPI4Py_INCLUDE_DIR)
    unset(MPI4Py_INCLUDE_DIR CACHE)
endif()
if(_MPI4Py_VERSION_RESULT)
    unset(MPI4Py_VERSION)
    unset(MPI4Py_VERSION CACHE)
endif()
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MPI4Py REQUIRED_VARS MPI4Py_INCLUDE_DIR MPI4Py_VERSION
                                  VERSION_VAR MPI4Py_VERSION)
