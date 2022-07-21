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

pybind11_add_module         (_lowfive lowfive.cpp)
target_link_libraries       (_lowfive PRIVATE ${libraries})
set_target_properties       (_lowfive PROPERTIES OUTPUT_NAME lowfive/_lowfive)
set_target_properties       (_lowfive PROPERTIES CXX_VISIBILITY_PRESET "default")

if                          (MPI4Py_FOUND)
    target_compile_definitions  (_lowfive PUBLIC "-DLOWFIVE_MPI4PY")
    target_include_directories  (_lowfive PUBLIC ${MPI4Py_INCLUDE_DIR})
endif()

