add_library             (lowfive
                            base/base.cpp
                            base/info.cpp
                            base/dataset.cpp
                            base/file.cpp
                            base/attr.cpp
                            base/group.cpp
                            base/introspect.cpp
                            vol-metadata.cpp
                            metadata/save-h5.cpp
                        )
target_link_libraries   (lowfive fmt::fmt ${HDF5_LIBRARY} diy)
target_compile_definitions(lowfive PRIVATE LOWFIVE_ENABLE_PASSTHRU_LOGGING)