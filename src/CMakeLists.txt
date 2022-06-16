if(lowfive_backward)
    set(LOWFIVE_BACKWARD_ENABLE ${BACKWARD_ENABLE})
else()
    set(LOWFIVE_BACKWARD_ENABLE "")
endif()

add_library             (lowfive SHARED
                            base/base.cpp
                            base/info.cpp
                            base/dataset.cpp
                            base/file.cpp
                            base/attr.cpp
                            base/group.cpp
                            base/introspect.cpp
                            base/wrap.cpp
                            base/object.cpp
                            base/blob.cpp
                            base/token.cpp
                            base/link.cpp
                            metadata/dataset.cpp
                            metadata/file.cpp
                            metadata/attr.cpp
                            metadata/group.cpp
                            metadata/introspect.cpp
                            metadata/wrap.cpp
                            metadata/object.cpp
                            metadata/blob.cpp
                            metadata/token.cpp
                            metadata/link.cpp
                            log.cpp
                            ${LOWFIVE_BACKWARD_ENABLE}
                        )

target_link_libraries     (lowfive fmt::fmt ${HDF5_LIBRARY})
target_include_directories(lowfive INTERFACE ${PROJECT_SOURCE_DIR}/include)


add_library             (lowfive-dist
                         metadata/save-h5.cpp           # not clear, if we even need this (doesn't seem to be used anywhere),
                                                        # but it requires DIY, so it's going here for now
                         dist/query.cpp
                         dist/index.cpp
                         vol-dist-metadata.cpp
                         ${LOWFIVE_BACKWARD_ENABLE})
target_link_libraries   (lowfive-dist fmt::fmt diy)

if(lowfive_backward)
    add_backward        (lowfive)
    add_backward        (lowfive-dist)
endif()


install                 (TARGETS lowfive lowfive-dist
                        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/
                        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                        GROUP_READ GROUP_WRITE GROUP_EXECUTE
                        WORLD_READ WORLD_WRITE WORLD_EXECUTE)
