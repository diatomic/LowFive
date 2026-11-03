# Small API cases run in separate processes to isolate the connector singleton.
function(lowfive_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE ${libraries})
    target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/src)
    foreach(case ${ARGN})
        add_test(NAME ${target}-${case} COMMAND ${target} ${case})
        set_tests_properties(${target}-${case} PROPERTIES TIMEOUT 30 LABELS api)
    endforeach()
endfunction()

lowfive_test(lowfive-api api.cpp memory file dual native patterns)
lowfive_test(lowfive-base base.cpp
    hard hard-same-dst hard-same-src copy-same-dst copy-same-src move-same-dst move-same-src
    reopen-native reopen-metadata exceptions property-lifetime info-independent
    info-nested info-malformed singleton distributed-close-failure)
lowfive_test(lowfive-factories factories.cpp
    base-metadata base-distributed metadata-distributed
    metadata-repeat distributed-repeat distributed-metadata)
lowfive_test(lowfive-request request.cpp
    singleton multiple cancel-free event-success event-failure
    submission-failure submission-request-failure forwarding-errors bounded-cleanup
    notify-singleton notify-immediate notify-out-of-order notify-error notify-partial
    notify-retry notify-exception wrap-failures)
lowfive_test(lowfive-serialization serialization.cpp invalid-root invalid-child)
lowfive_test(lowfive-datatypes datatypes.cpp binary object-info)
lowfive_test(lowfive-model model.cpp
    attribute-conversion attribute-compound attribute-strings dataset-conversion
    attribute-unwritten attribute-unwritten-serialization hid-moves
    dataspace-ownership compound-background compound-indirect-background
    dataset-fill dataset-fill-reference-serialization dataset-properties-stale-reference dataset-shrink
    read-all write-all scalar-selections selected-strings zerocopy-strings datatype-ownership
    persist-attributes persist-strings vlen-rewrite dataset-vlen-overlap
    dataset-vlen-serialization dataset-vlen-sparse-serialization shutdown-cleanup
    reference-top reference-compound reference-region-serialization
    hard-link-serialization file-copy-root-token reference-overwrite
    reference-delete dataset-reference-delete persist-references
    persist-cross-file-reference persist-overwritten-reference
    persist-deleted-reference-path-reuse persist-selected-vlen-reference
    persist-unwritten-attribute persist-properties persist-links
    persist-committed-types persist-fill-references persist-nested-fill-references
    persist-unsupported-object)
lowfive_test(lowfive-callbacks callbacks.cpp
    wildcard file_plist_ownership file_failure file_access file_exclusive
    missing_callback_names metadata_optional_contract file_owned_object_ids
    file_accessible bounded_object_ids datatype_failures
    committed_datatype_attributes group_plist_copy attr_iteration
    lifetime multiple_files dual_reopen link_name group_failure dataset_failure
    attr_failure missing_dataset tokens attr_rename hard_links soft_links
    link_copy_move absolute_paths attr_by_name native_parent_attr file_plists
    enumeration indexed_lookup object_names zero_file_name
    native_object_dataset_extent native_attr_iteration native_attr_open_delete
    native_attr_delete_idx native_attr_delete_idx_mixed native_link_delete
    native_link_delete_idx native_link_delete_idx_uncached native_link_copy_move
    native_link_move_cached native_link_move_cycle native_soft_link_uncached
    native_group_soft_link_uncached native_dataset_intermediate_soft_link
    native_object_soft_link_uncached extent_failure reference_ids lifetime_group
    lifetime_attr datatype_conversion dataset-metadata-get-failure
    native_datatype_attr native_datatype_object_open
    native_datatype_commit_parent
    native_datatype_link_create hard_link_unlink open_unlink_read
    anonymous_dataset_link detached-self-link
    file-id-root-group file-id-child file-id-child-file file-id-child-dual
    reference-region reference-cross-file reference-path-reuse
    reference-deleted-path-reuse reference-passthru-target
    reference-native-identity reference-native-identity-deferred
    reference-after-file-close reference-opened-native
    reference-external-native
    reference-registered-uncached reference-registered-root
    reference-registered-uncached-objects reference-registered-cached-objects
    reference-registered-intermediate-group
    reference-live-file-collection reference-retained-collection
    reference-attribute reference-native-attribute dataset-reference-overwrite
    reference-zero-copy-alias
    vlen-zero-copy-alias soft-link-cross-file
    intermediate-group intermediate-group-native
    intermediate-dataset intermediate-dataset-native
    intermediate-datatype intermediate-datatype-native
    intermediate-links intermediate-links-native)
lowfive_test(lowfive-examples examples.cpp
    serialization grid-valid grid-corrupt grid-read-error particles-valid particles-duplicate
    particles-reordered particles-coordinate particles-read-error)

function(lowfive_mpi_cases target)
    foreach(case ${ARGN})
        add_test(NAME ${target}-${case}
                 COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 ${MPIEXEC_PREFLAGS}
                         $<TARGET_FILE:${target}> ${MPIEXEC_POSTFLAGS} ${case})
        set_tests_properties(${target}-${case} PROPERTIES TIMEOUT 30 PROCESSORS 2 LABELS mpi)
    endforeach()
endfunction()
function(lowfive_mpi_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE ${libraries})
    target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/src)
    lowfive_mpi_cases(${target} ${ARGN})
endfunction()
lowfive_mpi_test(lowfive-distributed distributed.cpp
    contents repeated-open object-open conversion memory-all irregular multifile bounds
    empty mpio-resources missing native-missing remote-missing hard-link-replace
    hard-link-serialized object-hard-link-serialized remote-unlinked-alias
    hard-link-parent-relative object-alias-policy vlen vlen-compound
    vlen-compound-selection scalar no-index
    fill-unwritten fill-partial fill-default fill-vlen fill-string empty-string
    broadcast-replace dummy-real intercomm-indices serving-cleanup)
foreach(case copy-whole dummy-copy-whole)
    add_test(NAME lowfive-distributed-${case}
             COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 3 ${MPIEXEC_PREFLAGS}
                     $<TARGET_FILE:lowfive-distributed> ${MPIEXEC_POSTFLAGS} ${case})
    set_tests_properties(lowfive-distributed-${case} PROPERTIES
                         TIMEOUT 30 PROCESSORS 3 LABELS mpi)
endforeach()
lowfive_mpi_test(lowfive-rpc rpc.cpp destroy-first destroy-second blob-lifetime exception borrowed)
set(lowfive_saved_mpi_postflags ${MPIEXEC_POSTFLAGS})
set(MPIEXEC_POSTFLAGS --lowfive-mpi-postflag)
lowfive_mpi_cases(lowfive-distributed postflag-order)
set(MPIEXEC_POSTFLAGS ${lowfive_saved_mpi_postflags})

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
foreach                     (p 6 7 8)
    add_test                (NAME prod-con-multidata-no-shared-metadata-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 0 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-con-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 0 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
                            consumer-multidata-test.hx 1 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 1 0: memory file shared
endforeach()

# Space sharing currently broken everywhere
## space sharing only works under Linux, not Mac
#if(UNIX AND NOT APPLE)

#    # space sharing communicating through metadata
#    foreach                 (p 6 7 8)
#        add_test            (NAME prod-con-multidata-shared-metadata-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
#                            consumer-multidata-test.hx 1 0 1) # 1 0 1: memory file shared
#    endforeach()

#    # space sharing communicating through a file
#    foreach                 (p 6 7 8)
#        add_test            (NAME prod-con-multidata-shared-file-p${p}
#                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p} producer-multidata-test.hx
#                            consumer-multidata-test.hx 0 1 1) # 0 1 1: memory file shared
#    endforeach()

#    # space sharing communicating through metadata but also writing a file
#    foreach                 (p 6 7 8)
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
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 0 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-2cons-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 0 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 6 7 8)
    add_test                (NAME prod-2cons-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-prod-2cons.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer-multidata-test.hx consumer1-multidata-test.hx consumer2-multidata-test.hx 1 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 1 0: memory file shared
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
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 0 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 0 0: memory file shared
endforeach()

# space partitioning communicating through a file
foreach                     (p 6 7 8)
    add_test                (NAME 2prods-con-multidata-no-shared-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 0 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 0 1 0: memory file shared
endforeach()

# space partitioning communicating through metadata but also writing a file
foreach                     (p 6 7 8)
    add_test                (NAME 2prods-con-multidata-no-shared-metadata-file-p${p}
                            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test-2prods-con.sh ${CMAKE_CURRENT_BINARY_DIR} ${p}
                            producer1-multidata-test.hx producer2-multidata-test.hx consumer-multidata-test.hx 1 1 0 ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_PREFLAGS}) # 1 1 0: memory file shared
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

function(lowfive_loader_test name expected_path expected_symbol)
    add_test(NAME ${name}
        COMMAND ${CMAKE_COMMAND}
            "-Dcommand=${MPIEXEC_EXECUTABLE};${ARGN}"
            "-Dexpected_path=${expected_path}" "-Dexpected_symbol=${expected_symbol}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/check-loader.cmake)
    set_tests_properties(${name} PROPERTIES TIMEOUT 20 PROCESSORS 2 LABELS "mpi;loader")
endfunction()

foreach(driver prod-con-multidata-test prod-2cons-multidata-test 2prods-con-multidata-test)
    if(driver STREQUAL "2prods-con-multidata-test")
        set(producer_option --prod1_exec)
        set(producer_symbol producer1_f)
        set(task_options --prod2_exec $<TARGET_FILE:producer2-multidata-test>
                         --con_exec $<TARGET_FILE:consumer-multidata-test>)
    elseif(driver STREQUAL "prod-2cons-multidata-test")
        set(producer_option --prod_exec)
        set(producer_symbol producer_f)
        set(task_options --con1_exec $<TARGET_FILE:consumer1-multidata-test>
                         --con2_exec $<TARGET_FILE:consumer2-multidata-test>)
    else()
        set(producer_option --prod_exec)
        set(producer_symbol producer_f)
        set(task_options --con_exec $<TARGET_FILE:consumer-multidata-test>)
    endif()
    foreach(failure missing-library missing-symbol)
        if(failure STREQUAL "missing-library")
            set(library "${CMAKE_CURRENT_BINARY_DIR}/lowfive-missing-loader-task.hx")
        else()
            set(library $<TARGET_FILE:consumer-multidata-test>)
        endif()
        lowfive_loader_test("${driver}-loader-${failure}" "${library}" "${producer_symbol}"
            ${MPIEXEC_NUMPROC_FLAG} 2 ${MPIEXEC_PREFLAGS}
            $<TARGET_FILE:${driver}> ${MPIEXEC_POSTFLAGS} ${producer_option} "${library}" ${task_options})
        if(driver STREQUAL "prod-con-multidata-test")
            lowfive_loader_test("${driver}-loader-rank-local-${failure}" "${library}" "${producer_symbol}"
                ${MPIEXEC_NUMPROC_FLAG} 1 ${MPIEXEC_PREFLAGS}
                $<TARGET_FILE:${driver}> ${MPIEXEC_POSTFLAGS} --prod_exec "${library}"
                    --con_exec $<TARGET_FILE:consumer-multidata-test>
                : ${MPIEXEC_NUMPROC_FLAG} 1 ${MPIEXEC_PREFLAGS}
                $<TARGET_FILE:${driver}> ${MPIEXEC_POSTFLAGS} --prod_exec $<TARGET_FILE:producer-multidata-test>
                    --con_exec $<TARGET_FILE:consumer-multidata-test>)
        endif()
    endforeach()
endforeach()
