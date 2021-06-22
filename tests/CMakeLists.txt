add_executable              (prod-con-multidata-test    ../examples/prod-con-multidata/prod-con-multidata.cpp)
target_link_libraries       (prod-con-multidata-test    ${libraries})

add_library                 (producer-multidata-test SHARED ../examples/prod-con-multidata/producer-multidata.cpp)
target_link_libraries       (producer-multidata-test ${libraries})
set_target_properties       (producer-multidata-test PROPERTIES PREFIX "")
set_target_properties       (producer-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (consumer-multidata-test SHARED ../examples/prod-con-multidata/consumer-multidata.cpp)
target_link_libraries       (consumer-multidata-test ${libraries})
set_target_properties       (consumer-multidata-test PROPERTIES PREFIX "")
set_target_properties       (consumer-multidata-test PROPERTIES SUFFIX ".hx")

# space partitioning communicating through metadata
foreach                     (p 2 4 8)
add_test                    (NAME prod-con-multidata-no-shared-metadata-p${p}
                            COMMAND ${MPIEXEC} -np ${p} $<TARGET_FILE:prod-con-multidata-test> -r ./producer-multidata-test.hx -c ./consumer-multidata-test.hx
                            )
endforeach                  (p)

# space partitioning communicating through a file
foreach                     (p 2 4 8)
add_test                    (NAME prod-con-multidata-no-shared-file-p${p}
                            COMMAND ${MPIEXEC} -np ${p} $<TARGET_FILE:prod-con-multidata-test> -m 0 -f 1 -r ./producer-multidata-test.hx -c ./consumer-multidata-test.hx
                            )
endforeach                  (p)

# space partitioning communicating through metadata but also writing a file
foreach                     (p 2 4 8)
add_test                    (NAME prod-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${MPIEXEC} -np ${p} $<TARGET_FILE:prod-con-multidata-test> -m 1 -f 1 -r ./producer-multidata-test.hx -c ./consumer-multidata-test.hx
                            )
endforeach                  (p)

# space sharing only works under LINUX, not Mac
if(UNIX AND NOT APPLE)
    # space sharing communicating through metadata
    foreach                 (p 2 4 8)
    add_test                (NAME prod-con-multidata-shared-metadata-p${p}
                            COMMAND ${MPIEXEC} -np ${p} $<TARGET_FILE:prod-con-multidata-test> -s -r ./producer-multidata-test.hx -c ./consumer-multidata-test.hx
                            )
    endforeach              (p)
endif()
