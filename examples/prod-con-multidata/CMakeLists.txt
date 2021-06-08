set                         (targets    prod-con-multidata)

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
