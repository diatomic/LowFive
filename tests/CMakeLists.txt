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
    add_test                (NAME prod-con-multidata-no-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 0 0 # 1 0 0: memory file shared
                            )
endforeach                  (p)

# space partitioning communicating through a file
foreach                     (p 2 4 8)
    add_test                (NAME prod-con-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 0 1 0 # 0 1 0: memory file shared
                            )
endforeach                  (p)

# space partitioning communicating through metadata but also writing a file
foreach                     (p 2 4 8)
    add_test                (NAME prod-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 1 0 # 1 1 0: memory file shared
                            )
endforeach                  (p)

# space sharing only works under Linux, not Mac
if(UNIX AND NOT APPLE)

    # space sharing communicating through metadata
    foreach                 (p 2 4 8)
        add_test            (NAME prod-con-multidata-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 0 1 # 1 0 1: memory file shared
                            )
    endforeach              (p)

    # space sharing communicating through a file
    foreach                 (p 2 4 8)
        add_test            (NAME prod-con-multidata-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 0 1 1 # 0 1 1: memory file shared
                            )
    endforeach              (p)

    # space sharing communicating through metadata but also writing a file
    foreach                 (p 2 4 8)
        add_test            (NAME prod-con-multidata-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 1 1 # 1 1 1: memory file shared
                            )
    endforeach              (p)

endif()
