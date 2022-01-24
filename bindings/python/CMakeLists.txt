if                          (CMAKE_LIBRARY_OUTPUT_DIRECTORY)        # to work with pip
    set                     (MODULE_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
else                        ()
    set                     (MODULE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif                       ()

pybind11_add_module         (_lowfive lowfive.cpp)
target_link_libraries       (_lowfive PRIVATE ${libraries})
set_target_properties       (_lowfive PROPERTIES OUTPUT_NAME lowfive)
