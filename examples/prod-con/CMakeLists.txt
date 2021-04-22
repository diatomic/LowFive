set                         (targets    prod-con)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

add_library                 (producer SHARED producer.cpp)
target_link_libraries       (producer ${libraries})
set_target_properties       (producer PROPERTIES PREFIX "")
set_target_properties       (producer PROPERTIES SUFFIX ".hx")

add_library                 (consumer SHARED consumer.cpp)
target_link_libraries       (consumer ${libraries})
set_target_properties       (consumer PROPERTIES PREFIX "")
set_target_properties       (consumer PROPERTIES SUFFIX ".hx")
