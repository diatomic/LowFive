add_library                 (producer-multidata-test SHARED ../examples/prod-con-multidata/producer-multidata.cpp)
target_link_libraries       (producer-multidata-test ${libraries})
set_target_properties       (producer-multidata-test PROPERTIES PREFIX "")
set_target_properties       (producer-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (producer1-multidata-test SHARED ../examples/prod-con-multidata/producer1-multidata.cpp)
target_link_libraries       (producer1-multidata-test ${libraries})
set_target_properties       (producer1-multidata-test PROPERTIES PREFIX "")
set_target_properties       (producer1-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (producer2-multidata-test SHARED ../examples/prod-con-multidata/producer2-multidata.cpp)
target_link_libraries       (producer2-multidata-test ${libraries})
set_target_properties       (producer2-multidata-test PROPERTIES PREFIX "")
set_target_properties       (producer2-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (consumer-multidata-test SHARED ../examples/prod-con-multidata/consumer-multidata.cpp)
target_link_libraries       (consumer-multidata-test ${libraries})
set_target_properties       (consumer-multidata-test PROPERTIES PREFIX "")
set_target_properties       (consumer-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (consumer1-multidata-test SHARED ../examples/prod-con-multidata/consumer1-multidata.cpp)
target_link_libraries       (consumer1-multidata-test ${libraries})
set_target_properties       (consumer1-multidata-test PROPERTIES PREFIX "")
set_target_properties       (consumer1-multidata-test PROPERTIES SUFFIX ".hx")

add_library                 (consumer2-multidata-test SHARED ../examples/prod-con-multidata/consumer2-multidata.cpp)
target_link_libraries       (consumer2-multidata-test ${libraries})
set_target_properties       (consumer2-multidata-test PROPERTIES PREFIX "")
set_target_properties       (consumer2-multidata-test PROPERTIES SUFFIX ".hx")

# ------------ test 1 producer 1 consumer ------------------

add_executable              (prod-con-multidata-test    ../examples/prod-con-multidata/prod-con-multidata.cpp)
target_link_libraries       (prod-con-multidata-test    ${libraries})

# space partitioning communicating through metadata
foreach                     (p 2 4 8)
    add_test                (NAME prod-con-multidata-no-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 0 0) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 2 4 8)
    add_test                (NAME prod-con-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 0 1 0) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 2 4 8)
    add_test                (NAME prod-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 1 0) # 1 1 0: memory file shared
endforeach()

# Space sharing currently broken everywhere
## space sharing only works under Linux, not Mac
#if(UNIX AND NOT APPLE)

#    # space sharing communicating through metadata
#    foreach                 (p 2 4 8)
#        add_test            (NAME prod-con-multidata-shared-metadata-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
#                            consumer-multidata-test.hx 1 0 1) # 1 0 1: memory file shared
#    endforeach()

#    # space sharing communicating through a file
#    foreach                 (p 2 4 8)
#        add_test            (NAME prod-con-multidata-shared-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
#                            consumer-multidata-test.hx 0 1 1) # 0 1 1: memory file shared
#    endforeach()

#    # space sharing communicating through metadata but also writing a file
#    foreach                 (p 2 4 8)
#        add_test            (NAME prod-con-multidata-shared-metadata-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
#                            consumer-multidata-test.hx 1 1 1) # 1 1 1: memory file shared
#    endforeach()

#endif()

# ------------ test 1 producer 2 consumers -----------------

add_executable              (prod-2cons-multidata-test    ../examples/prod-con-multidata/prod-2cons-multidata.cpp)
target_link_libraries       (prod-2cons-multidata-test    ${libraries})

# space partitioning communicating through metadata
foreach                     (p 6 7 8)
    add_test                (NAME prod-2cons-multidata-no-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 0 0) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-2cons-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 0 1 0) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-2cons-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 1 0) # 1 1 0: memory file shared
endforeach()

# Space sharing currently broken everywhere
## space sharing only works under Linux, not Mac
#if(UNIX AND NOT APPLE)

#    # space sharing communicating through metadata
#    foreach                 (p 6 7 8)
#        add_test            (NAME prod-2cons-multidata-shared-metadata-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 0 1) # 1 0 1: memory file shared
#    endforeach()

#    # space sharing communicating through a file
#    foreach                 (p 6 7 8)
#        add_test            (NAME prod-2cons-multidata-shared-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 0 1 1) # 0 1 1: memory file shared
#    endforeach()

#    # space sharing communicating through metadata but also writing a file
#    foreach                 (p 6 7 8)
#        add_test            (NAME prod-2cons-multidata-shared-metadata-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 1 1) # 1 1 1: memory file shared
#    endforeach()

#endif()

# ------------ test 2 producers 1 consumer -----------------

add_executable              (2prods-con-multidata-test    ../examples/prod-con-multidata/2prods-con-multidata.cpp)
target_link_libraries       (2prods-con-multidata-test    ${libraries})

# space partitioning communicating through metadata
foreach                     (p 6 7 8)
    add_test                (NAME 2prods-con-multidata-no-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 0 0) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 6 7 8)
    add_test                (NAME 2prods-con-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 0 1 0) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 6 7 8)
    add_test                (NAME 2prods-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 1 0) # 1 1 0: memory file shared
endforeach()

# Space sharing currently broken everywhere
## space sharing only works under Linux, not Mac
#if(UNIX AND NOT APPLE)

#    # space sharing communicating through metadata
#    foreach                 (p 6 7 8)
#        add_test            (NAME 2prods-con-multidata-shared-metadata-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 0 1) # 1 0 1: memory file shared
#    endforeach()

#    # space sharing communicating through a file
#    foreach                 (p 6 7 8)
#        add_test            (NAME 2prods-con-multidata-shared-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 0 1 1) # 0 1 1: memory file shared
#    endforeach()

#    # space sharing communicating through metadata but also writing a file
#    foreach                 (p 6 7 8)
#        add_test            (NAME 2prods-con-multidata-shared-metadata-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
#                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 1 1) # 1 1 1: memory file shared
#    endforeach()

#endif()

