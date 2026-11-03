set                         (targets    attr-iterate)

foreach                     (t ${targets})
  add_executable            (${t} ${t}.cpp)
  target_link_libraries     (${t} ${libraries})
endforeach                  ()

if                          (lowfive_install_examples)
    install                 (TARGETS attr-iterate
                            DESTINATION examples/attributes
                            )
endif                       ()
