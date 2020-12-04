add_library             (lowfive vol-base.cpp vol-metadata.cpp metadata/save-h5.cpp)
target_link_libraries   (lowfive fmt::fmt ${HDF5_LIBRARY})
target_compile_definitions(lowfive PRIVATE ENABLE_PASSTHRU_LOGGING)
