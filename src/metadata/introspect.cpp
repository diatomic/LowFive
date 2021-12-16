#include <lowfive/vol-metadata.hpp>

herr_t
LowFive::MetadataVOL::
introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    return VOLBase::introspect_get_conn_cls(unwrap(obj), lvl, conn_cls);
}

herr_t
LowFive::MetadataVOL::
introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported)
{
    return VOLBase::introspect_opt_query(unwrap(obj), cls, opt_type, supported);
}

