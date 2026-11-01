set                         (targets    group-iterate)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

if                          (lowfive_install_examples)
    install                 (TARGETS group-iterate
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/groups/
                            )
endif                       ()
