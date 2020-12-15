set                         (targets    ghost)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})

  install                   (TARGETS ${t}
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/ghost
                            PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                            GROUP_READ GROUP_WRITE GROUP_EXECUTE
                            WORLD_READ WORLD_WRITE WORLD_EXECUTE)
endforeach                  ()
