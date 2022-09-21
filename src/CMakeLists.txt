if(lowfive_backward)
    set(LOWFIVE_BACKWARD_ENABLE ${BACKWARD_ENABLE})
else()
    set(LOWFIVE_BACKWARD_ENABLE "")
endif()

add_library             (lowfive SHARED
                            vol-base/base.cpp
                            vol-base/info.cpp
                            vol-base/dataset.cpp
                            vol-base/file.cpp
                            vol-base/attr.cpp
                            vol-base/group.cpp
                            vol-base/introspect.cpp
                            vol-base/wrap.cpp
                            vol-base/object.cpp
                            vol-base/blob.cpp
                            vol-base/token.cpp
                            vol-base/link.cpp
                            vol-metadata.cpp
                            vol-metadata/dataset.cpp
                            vol-metadata/file.cpp
                            vol-metadata/attr.cpp
                            vol-metadata/group.cpp
                            vol-metadata/introspect.cpp
                            vol-metadata/wrap.cpp
                            vol-metadata/object.cpp
                            vol-metadata/blob.cpp
                            vol-metadata/token.cpp
                            vol-metadata/link.cpp
                            vol-metadata/serialization.cpp
                            log.cpp
                            ${LOWFIVE_BACKWARD_ENABLE}
                        )

target_link_libraries     (lowfive fmt::fmt ${HDF5_LIBRARY} diy)
target_include_directories(lowfive INTERFACE ${PROJECT_SOURCE_DIR}/include)


add_library             (lowfive-dist
                         vol-metadata/save-h5.cpp       # not clear, if we even need this (doesn't seem to be used anywhere),
                                                        # but it requires DIY, so it's going here for now
                         vol-dist/query.cpp
                         vol-dist/index.cpp
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
