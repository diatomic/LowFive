add_library             (lowfive vol-base.cpp vol-metadata.cpp)
target_link_libraries   (lowfive fmt::fmt)
target_compile_definitions(lowfive PRIVATE ENABLE_PASSTHRU_LOGGING)
