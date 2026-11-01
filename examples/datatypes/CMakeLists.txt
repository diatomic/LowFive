set                         (targets    datatypes)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

if                          (lowfive_install_examples)
    install                 (TARGETS datatypes
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/datatypes/
                            )
endif                       ()
