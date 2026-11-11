if(lowfive_backward)
    set(LOWFIVE_BACKWARD_ENABLE ${BACKWARD_ENABLE})
else()
    set(LOWFIVE_BACKWARD_ENABLE "")
endif()

add_library             (lowfive SHARED
                            vol-base/base.cpp
                            vol-base/info.cpp
                            vol-base/dataset.cpp
                            vol-base/datatype.cpp
                            vol-base/file.cpp
                            vol-base/attr.cpp
                            vol-base/group.cpp
                            vol-base/introspect.cpp
                            vol-base/wrap.cpp
                            vol-base/request.cpp
                            vol-base/object.cpp
                            vol-base/blob.cpp
                            vol-base/token.cpp
                            vol-base/link.cpp
                            vol-metadata.cpp
                            vol-metadata/dataset.cpp
                            vol-metadata/datatype.cpp
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

target_link_libraries     (lowfive
                           PRIVATE
                           fmt::fmt-header-only
                           spdlog::spdlog_header_only
                           diy
                           PUBLIC
                           ${HDF5_LIBRARY})
target_include_directories(lowfive INTERFACE ${PROJECT_SOURCE_DIR}/include)


add_library             (lowfive-dist
                         vol-metadata/save-h5.cpp       # not clear, if we even need this (doesn't seem to be used anywhere),
                                                        # but it requires DIY, so it's going here for now
                         vol-dist/query.cpp
                         vol-dist/index.cpp
                         vol-dist-metadata.cpp
                         ${LOWFIVE_BACKWARD_ENABLE})
target_link_libraries   (lowfive-dist PRIVATE
                         fmt::fmt-header-only
                         spdlog::spdlog_header_only
                         diy)
if                      (TARGET MPI::MPI_CXX)
    target_link_libraries(lowfive-dist PUBLIC MPI::MPI_CXX)
endif                   ()

if(lowfive_backward)
    add_backward        (lowfive)
    add_backward        (lowfive-dist)
endif()


install                 (TARGETS lowfive lowfive-dist
                        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
