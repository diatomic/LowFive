set                         (targets    attr-iterate)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

if                          (lowfive_install_examples)
    install                 (TARGETS attr-iterate
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/attributes/
                            PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                            GROUP_READ GROUP_WRITE GROUP_EXECUTE
                            WORLD_READ WORLD_WRITE WORLD_EXECUTE)
endif                       ()
