set                         (targets    dimension-scales dimension-scales-no-lowfive)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

if                          (lowfive_install_examples)
    install                 (TARGETS dimension-scales
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/dimension-scales/
                            )
endif                       ()
