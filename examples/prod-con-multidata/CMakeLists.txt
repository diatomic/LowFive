set                         (targets    prod-con-multidata prod-2cons-multidata 2prods-con-multidata)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

add_library                 (producer-multidata SHARED producer-multidata.cpp)
target_link_libraries       (producer-multidata ${libraries})
set_target_properties       (producer-multidata PROPERTIES PREFIX "")
set_target_properties       (producer-multidata PROPERTIES SUFFIX ".hx")

add_library                 (consumer-multidata SHARED consumer-multidata.cpp)
target_link_libraries       (consumer-multidata ${libraries})
set_target_properties       (consumer-multidata PROPERTIES PREFIX "")
set_target_properties       (consumer-multidata PROPERTIES SUFFIX ".hx")

add_library                 (producer1-multidata SHARED producer1-multidata.cpp)
target_link_libraries       (producer1-multidata ${libraries})
set_target_properties       (producer1-multidata PROPERTIES PREFIX "")
set_target_properties       (producer1-multidata PROPERTIES SUFFIX ".hx")

add_library                 (producer2-multidata SHARED producer2-multidata.cpp)
target_link_libraries       (producer2-multidata ${libraries})
set_target_properties       (producer2-multidata PROPERTIES PREFIX "")
set_target_properties       (producer2-multidata PROPERTIES SUFFIX ".hx")

add_library                 (consumer1-multidata SHARED consumer1-multidata.cpp)
target_link_libraries       (consumer1-multidata ${libraries})
set_target_properties       (consumer1-multidata PROPERTIES PREFIX "")
set_target_properties       (consumer1-multidata PROPERTIES SUFFIX ".hx")

add_library                 (consumer2-multidata SHARED consumer2-multidata.cpp)
target_link_libraries       (consumer2-multidata ${libraries})
set_target_properties       (consumer2-multidata PROPERTIES PREFIX "")
set_target_properties       (consumer2-multidata PROPERTIES SUFFIX ".hx")

if                          (lowfive_install_examples)
    install                 (TARGETS
                            prod-con-multidata prod-2cons-multidata 2prods-con-multidata
                            producer-multidata producer1-multidata producer2-multidata
                            consumer-multidata consumer1-multidata consumer2-multidata
                            DESTINATION ${CMAKE_INSTALL_PREFIX}/examples/prod-con-multidata/
                            PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                            GROUP_READ GROUP_WRITE GROUP_EXECUTE
                            WORLD_READ WORLD_WRITE WORLD_EXECUTE)
endif                       ()

add_executable              (prod-metadata-only prod-metadata-only.cpp)
target_link_libraries       (prod-metadata-only ${libraries})
set_target_properties       (prod-metadata-only PROPERTIES PREFIX "")
