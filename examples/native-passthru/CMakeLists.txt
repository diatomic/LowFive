set                         (targets    native-passthru)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp ../../src/our-passthru-plugin.c)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()
