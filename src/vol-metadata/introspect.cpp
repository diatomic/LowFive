#include <lowfive/vol-metadata.hpp>
#include "../log-private.hpp"

herr_t
LowFive::MetadataVOL::
introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    auto logger = get_logger();

    if (logger) logger->trace("MetadataVOL::introspect_get_conn_cls, obj = {}", fmt::ptr(obj));

    // for now, always return our connector for all lvl-s
    *conn_cls = &connector;
    return 0;

//   TODO: maybe, we need something like this? If TERM connector must be native?

//    if (lvl == H5VL_GET_CONN_LVL_TERM && unwrap(obj)) {
//        // If there is a corresponding native object, return it (VOLBase calls native HDF5 function)
//        if (logger) logger->trace("MetadataVOL::introspect_get_conn_cls, {}, unwrap(obj) = {}", fmt::ptr(obj), fmt::ptr(unwrap(obj)));
//        return VOLBase::introspect_get_conn_cls(unwrap(obj), lvl, conn_cls);
//        return 0;
//    } else {
//        *conn_cls = &connector;
//        return 0;
//    }
}

herr_t
LowFive::MetadataVOL::
introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported, uint64_t *flags)
{
    return VOLBase::introspect_opt_query(unwrap(obj), cls, opt_type, supported, flags);
}

