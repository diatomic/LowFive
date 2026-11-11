if                          (CMAKE_LIBRARY_OUTPUT_DIRECTORY)        # to work with pip
    set                     (MODULE_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
else                        ()
    set                     (MODULE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif                       ()

list                        (APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
find_package                (MPI4Py)

# Copy lowfive/*.py
file                        (MAKE_DIRECTORY "${MODULE_OUTPUT_DIRECTORY}/lowfive")
file                        (GLOB LOWFIVE_PYTHON "${CMAKE_CURRENT_SOURCE_DIR}/lowfive/*.py")
add_custom_target           (pylowfive ALL ${CMAKE_COMMAND} -E copy_directory
                                    ${CMAKE_CURRENT_SOURCE_DIR}/lowfive ${MODULE_OUTPUT_DIRECTORY}/lowfive
                                    DEPENDS ${LOWFIVE_PYTHON})
configure_file              (${CMAKE_CURRENT_SOURCE_DIR}/test-dist.py
                             ${MODULE_OUTPUT_DIRECTORY}/test-dist.py COPYONLY)

pybind11_add_module         (_lowfive lowfive.cpp)
target_link_libraries       (_lowfive PRIVATE ${libraries})
set_target_properties       (_lowfive PROPERTIES OUTPUT_NAME lowfive/_lowfive)
set_target_properties       (_lowfive PROPERTIES CXX_VISIBILITY_PRESET "default")

if                          (MPI4Py_FOUND)
    target_compile_definitions  (_lowfive PUBLIC "-DLOWFIVE_MPI4PY")
    target_include_directories  (_lowfive PUBLIC ${MPI4Py_INCLUDE_DIR})
endif()

# Use the same interpreter for compilation, MPI bindings, tests, and installation.
execute_process             (COMMAND "${Python_EXECUTABLE}" -c
                             "import sysconfig; print(sysconfig.get_path('platlib', vars={'base': '', 'platbase': ''}).lstrip('/'))"
                             OUTPUT_VARIABLE LOWFIVE_PYTHON_INSTALL_DIR
                             OUTPUT_STRIP_TRAILING_WHITESPACE
                             RESULT_VARIABLE LOWFIVE_PYTHON_PATH_STATUS)
if                          (NOT LOWFIVE_PYTHON_PATH_STATUS EQUAL 0)
    message                 (FATAL_ERROR "Could not determine the Python installation directory")
endif                       ()
install                     (TARGETS _lowfive LIBRARY DESTINATION ${LOWFIVE_PYTHON_INSTALL_DIR}/lowfive)
install                     (FILES lowfive/__init__.py DESTINATION ${LOWFIVE_PYTHON_INSTALL_DIR}/lowfive)

foreach                     (case exports errors lifetime)
    add_test                (NAME lowfive-python-${case}
                             COMMAND "${CMAKE_COMMAND}" -E env
                             "HDF5_PLUGIN_PATH=$<TARGET_FILE_DIR:lowfive>"
                             "${Python_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tests/prepend_python_path.py"
                             "${MODULE_OUTPUT_DIRECTORY}"
                             "${PROJECT_SOURCE_DIR}/tests/python_api.py" ${case})
    set_tests_properties    (lowfive-python-${case} PROPERTIES TIMEOUT 30 LABELS python)
endforeach                  ()
if                          (MPI4Py_FOUND)
    execute_process         (COMMAND "${Python_EXECUTABLE}" -c "import h5py, numpy"
                             RESULT_VARIABLE LOWFIVE_H5PY_STATUS
                             ERROR_QUIET)
    if                      (LOWFIVE_H5PY_STATUS EQUAL 0)
        add_test            (NAME lowfive-python-distributed
                             COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 3 ${MPIEXEC_PREFLAGS}
                             "${CMAKE_COMMAND}" -E env
                             "HDF5_PLUGIN_PATH=$<TARGET_FILE_DIR:lowfive>"
                             "HDF5_VOL_CONNECTOR=lowfive under_vol=0$<SEMICOLON>under_info={}$<SEMICOLON>"
                             "${Python_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tests/prepend_python_path.py"
                             "${MODULE_OUTPUT_DIRECTORY}"
                             "${MODULE_OUTPUT_DIRECTORY}/test-dist.py" -m
                             ${MPIEXEC_POSTFLAGS})
        set_tests_properties(lowfive-python-distributed PROPERTIES
                             TIMEOUT 60 LABELS "python;mpi" PROCESSORS 3)
        add_test            (NAME lowfive-python-distributed-file
                             COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS}
                             "${CMAKE_COMMAND}" -E env
                             "HDF5_PLUGIN_PATH=$<TARGET_FILE_DIR:lowfive>"
                             "HDF5_VOL_CONNECTOR=lowfive under_vol=0$<SEMICOLON>under_info={}$<SEMICOLON>"
                             "${Python_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tests/prepend_python_path.py"
                             "${MODULE_OUTPUT_DIRECTORY}"
                             "${MODULE_OUTPUT_DIRECTORY}/test-dist.py" -f
                             ${MPIEXEC_POSTFLAGS})
        set_tests_properties(lowfive-python-distributed-file PROPERTIES
                             TIMEOUT 60 LABELS "python;mpi" PROCESSORS 4)
    endif                   ()
endif                       ()
set                         (lowfive_install_example_args)
if                          (lowfive_install_examples)
    list                    (APPEND lowfive_install_example_args
                             "--file=examples/attributes/$<TARGET_FILE_NAME:attr-iterate>"
                             "--file=examples/datatypes/$<TARGET_FILE_NAME:datatypes>"
                             "--file=examples/dimension-scales/$<TARGET_FILE_NAME:dimension-scales>"
                             "--file=examples/groups/$<TARGET_FILE_NAME:group-iterate>"
                             "--file=examples/links/$<TARGET_FILE_NAME:links>"
                             "--file=examples/location/$<TARGET_FILE_NAME:location>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:prod-con-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:prod-2cons-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:2prods-con-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:producer-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:producer1-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:producer2-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:consumer-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:consumer1-multidata>"
                             "--file=examples/prod-con-multidata/$<TARGET_FILE_NAME:consumer2-multidata>")
endif                       ()
foreach                     (case permissions python relocatable)
    add_test                (NAME lowfive-install-${case}
                             COMMAND "${Python_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tests/install.py"
                             "${PROJECT_BINARY_DIR}" "${CMAKE_INSTALL_PREFIX}" ${case}
                             "--libdir=${CMAKE_INSTALL_LIBDIR}"
                             "--bindir=${CMAKE_INSTALL_BINDIR}"
                             "--includedir=${CMAKE_INSTALL_INCLUDEDIR}"
                             "$<TARGET_FILE_NAME:lowfive>" "$<TARGET_FILE_NAME:lowfive-dist>"
                             ${lowfive_install_example_args})
    set_tests_properties    (lowfive-install-${case} PROPERTIES TIMEOUT 60 LABELS install)
endforeach                  ()
set                         (lowfive_public_header_include_dirs
                             ${HDF5_INCLUDE_DIR}
                             ${HDF5_INCLUDE_DIRS}
                             ${MPI_C_INCLUDE_PATH}
                             ${MPI_C_INCLUDE_DIRS})
list                        (REMOVE_DUPLICATES lowfive_public_header_include_dirs)
set                         (lowfive_public_header_args)
foreach                     (include_dir IN LISTS lowfive_public_header_include_dirs)
    list                    (APPEND lowfive_public_header_args "--include=${include_dir}")
endforeach                  ()
add_test                    (NAME lowfive-install-public-header
                             COMMAND "${Python_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tests/install.py"
                             "${PROJECT_BINARY_DIR}" "${CMAKE_INSTALL_PREFIX}" public-header
                             "--libdir=${CMAKE_INSTALL_LIBDIR}"
                             "--bindir=${CMAKE_INSTALL_BINDIR}"
                             "--includedir=${CMAKE_INSTALL_INCLUDEDIR}"
                             "--compiler=${CMAKE_CXX_COMPILER}"
                             "--toolchain=${CMAKE_TOOLCHAIN_FILE}"
                             ${lowfive_public_header_args})
set_tests_properties        (lowfive-install-public-header PROPERTIES TIMEOUT 60 LABELS install)
add_test                    (NAME lowfive-cmake-mpi4py-stale-cache
                             COMMAND "${CMAKE_COMMAND}"
                             "-DMPI4PY_FINDER=${CMAKE_CURRENT_SOURCE_DIR}/FindMPI4Py.cmake"
                             -P "${PROJECT_SOURCE_DIR}/tests/find_mpi4py.cmake")
set_tests_properties        (lowfive-cmake-mpi4py-stale-cache PROPERTIES
                             TIMEOUT 30 LABELS python)


