#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include "../log-private.hpp"
#include <exception>
#include <stdexcept>
#include <type_traits>

namespace
{
// Every function registered with HDF5 must return through its C frame normally.
template<auto Function> struct Callback;
template<class Result, class... Arguments, Result (*Function)(Arguments...)>
struct Callback<Function>
{
    static Result invoke(Arguments... arguments) noexcept
    {
        try { return Function(arguments...); }
        catch (const std::exception& error)
        {
            H5Epush2(H5E_DEFAULT, __FILE__, "LowFive callback", __LINE__,
                     H5E_ERR_CLS, H5E_VOL, H5E_CALLBACK, "%s", error.what());
        }
        catch (...)
        {
            H5Epush2(H5E_DEFAULT, __FILE__, "LowFive callback", __LINE__,
                     H5E_ERR_CLS, H5E_VOL, H5E_CALLBACK, "%s", "unknown C++ exception");
        }
        if constexpr (std::is_pointer_v<Result>) return nullptr;
        else return static_cast<Result>(-1);
    }
};
}

hid_t LowFive::VOLBase::connector_id = -1;

H5VL_class_t LowFive::VOLBase::connector =
        {
                3,                                              /* version, must be 2 for HDF5 1.13 and 3 for HDF5 1.14 */
                (H5VL_class_value_t) 510,                       /* value        */
                "lowfive",                                      /* name         */
                0,                                              /* connector version */
                _cap_flags,                                    /* capability flags */
                &Callback<&_init>::invoke,                       /* initialize   */
                &Callback<&_term>::invoke,                       /* terminate    */
                {                                               /* info_cls */
                        sizeof(info_t),                             /* size    */
                        &Callback<&_info_copy>::invoke,                                /* copy    */
                        NULL, //&OUR_pass_through_info_cmp,                  /* compare */
                        &Callback<&_info_free>::invoke,                                /* free    */
                        &Callback<&_info_to_str>::invoke,                              /* to_str  */
                        &Callback<&_str_to_info>::invoke                               /* from_str */
                },
                {                                           /* wrap_cls */
                        &Callback<&_wrap_get_object>::invoke,                          /* get_object   */
                        &Callback<&_get_wrap_ctx>::invoke,                             /* get_wrap_ctx */
                        &Callback<&_wrap_object>::invoke,                              /* wrap_object  */
                        &Callback<&_unwrap_object>::invoke,                            /* unwrap_object */
                        &Callback<&_free_wrap_ctx>::invoke                             /* free_wrap_ctx */
                },
                {                                           /* attribute_cls */
                        &Callback<&_attr_create>::invoke,                              /* create */
                        &Callback<&_attr_open>::invoke,                                /* open */
                        &Callback<&_attr_read>::invoke,                                /* read */
                        &Callback<&_attr_write>::invoke,                               /* write */
                        &Callback<&_attr_get>::invoke,            /* get */
                        &Callback<&_attr_specific>::invoke,                            /* specific */
                        &Callback<&_attr_optional>::invoke,                            /* optional */
                        &Callback<&_attr_close>::invoke                                /* close */
                },
                {                                           /* dataset_cls */
                        &Callback<&_dataset_create>::invoke,                           /* create */
                        &Callback<&_dataset_open>::invoke,                             /* open */
                        &Callback<&_dataset_read>::invoke,                             /* read */
                        &Callback<&_dataset_write>::invoke,                            /* write */
                        &Callback<&_dataset_get>::invoke,                              /* get */
                        &Callback<&_dataset_specific>::invoke,                         /* specific */
                        &Callback<&_dataset_optional>::invoke,                         /* optional */
                        &Callback<&_dataset_close>::invoke                             /* close */
                },
                {                                           /* datatype_cls */
                        &Callback<&_datatype_commit>::invoke,                          /* commit */
                        &Callback<&_datatype_open>::invoke,                            /* open */
                        &Callback<&_datatype_get>::invoke,                             /* get_size */
                        &Callback<&_datatype_specific>::invoke,                        /* specific */
                        &Callback<&_datatype_optional>::invoke,                        /* optional */
                        &Callback<&_datatype_close>::invoke                            /* close */
                },
                {                                           /* file_cls */
                        &Callback<&_file_create>::invoke,                               /* create */
                        &Callback<&_file_open>::invoke,                                 /* open */
                        &Callback<&_file_get>::invoke,            /* get */
                        &Callback<&_file_specific>::invoke,                             /* specific */
                        &Callback<&_file_optional>::invoke,                             /* optional */
                        &Callback<&_file_close>::invoke                                 /* close */
                },
                {                                           /* group_cls */
                        &Callback<&_group_create>::invoke,                              /* create */
                        &Callback<&_group_open>::invoke,                                /* open */
                        &Callback<&_group_get>::invoke,                                 /* get */
                        &Callback<&_group_specific>::invoke,                            /* specific */
                        &Callback<&_group_optional>::invoke,                            /* optional */
                        &Callback<&_group_close>::invoke                                /* close */
                },
                {                                           /* link_cls */
                        &Callback<&_link_create>::invoke,                              /* create */
                        &Callback<&_link_copy>::invoke,                                /* copy */
                        &Callback<&_link_move>::invoke,                                /* move */
                        &Callback<&_link_get>::invoke,            /* get */
                        &Callback<&_link_specific>::invoke,                            /* specific */
                        &Callback<&_link_optional>::invoke                             /* optional */
                },
                {                                           /* object_cls */
                        &Callback<&_object_open>::invoke,                              /* open */
                        &Callback<&_object_copy>::invoke,                              /* copy */
                        &Callback<&_object_get>::invoke,                               /* get */
                        &Callback<&_object_specific>::invoke,                          /* specific */
                        &Callback<&_object_optional>::invoke                           /* optional */
                },
                {                                           /* introspect_cls */
                        &Callback<&_introspect_get_conn_cls>::invoke,                   /* get_conn_cls */
                        &Callback<&_introspect_get_cap_flags>::invoke,                  /* get_cap_flags */
                        &Callback<&_introspect_opt_query>::invoke                       /* opt_query */
                },
                {                                           /* request_cls */
                        &Callback<&_request_wait>::invoke,              /* wait */
                        &Callback<&_request_notify>::invoke,            /* notify */
                        &Callback<&_request_cancel>::invoke,            /* cancel */
                        &Callback<&_request_specific>::invoke,          /* specific */
                        &Callback<&_request_optional>::invoke,          /* optional */
                        &Callback<&_request_free>::invoke               /* free */
                },
                {                                                       /* blob_cls */
                        &Callback<&_blob_put>::invoke,            /* put */
                        &Callback<&_blob_get>::invoke,            /* get */
                        &Callback<&_blob_specific>::invoke,                                    /* specific */
                        NULL, // OUR_pass_through_blob_optional             /* optional */
                },
                {                                           /* token_cls */
                        &Callback<&_token_cmp>::invoke,                                 /* cmp */
                        NULL, // OUR_pass_through_token_to_str,             /* to_str */
                        NULL, // OUR_pass_through_token_from_str              /* from_str */
                },
                &Callback<&_optional>::invoke                    /* optional */
        };

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_VOL; }
const void *H5PLget_plugin_info(void)
{
    return &LowFive::VOLBase::connector;
}

LowFive::VOLBase::
VOLBase()
{
    auto log = get_logger();
    log->trace("VOLBase::VOLBase(), &info = {}\n", fmt::ptr(&info));

    // Decode into a separately owned live configuration, never into caller info.
    const hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (plist < 0)
        throw std::runtime_error("cannot create LowFive initialization FAPL");
    if (!info)
    {
        hid_t configured_connector = H5I_INVALID_HID;
        H5VL_class_value_t configured_value = static_cast<H5VL_class_value_t>(-1);
        if (H5Pget_vol_id(plist, &configured_connector) >= 0)
        {
            H5VLget_value(configured_connector, &configured_value);
            if (configured_value == connector.value)
                H5Pget_vol_info(plist, reinterpret_cast<void**>(&info));
            H5Idec_ref(configured_connector);
        }
        if (!info)
        {
            info = new info_t{H5VL_NATIVE, nullptr};
            H5Iinc_ref(info->under_vol_id);
        }
    }
    H5Pclose(plist);

    info_t::vol = this;
}

void
LowFive::VOLBase::
track_wrapped_object(pass_through_t* object)
{
    object->wrapped_next = wrapped_objects;
    if (wrapped_objects)
        wrapped_objects->wrapped_previous = object;
    wrapped_objects = object;
}

void
LowFive::VOLBase::
untrack_wrapped_object(pass_through_t* object)
{
    if (object->wrapped_previous)
        object->wrapped_previous->wrapped_next = object->wrapped_next;
    else if (wrapped_objects == object)
        wrapped_objects = object->wrapped_next;
    if (object->wrapped_next)
        object->wrapped_next->wrapped_previous = object->wrapped_previous;
    object->wrapped_previous = nullptr;
    object->wrapped_next = nullptr;
}

void
LowFive::VOLBase::
release_wrapped_objects()
{
    while (wrapped_objects)
    {
        auto* object = wrapped_objects;
        if (!_unwrap_object(object))
        {
            untrack_wrapped_object(object);
            pass_through_t::destroy(object);
        }
    }
}

LowFive::VOLBase::
~VOLBase()
{
    release_wrapped_objects();
    if (info_t::vol == this)
        info_t::vol = nullptr;
}

hid_t
LowFive::VOLBase::
register_plugin()
{
    auto log = get_logger();
    log->trace("registering plugin, info = {}\n", fmt::ptr(info));

    // Registration reuses HDF5's connector while acquiring an application
    // reference for each owner. HDF5 releases the singleton at the final close.
    connector_id = H5VLregister_connector(&connector, H5P_DEFAULT);

    return connector_id;
}

/*-------------------------------------------------------------------------
 * Function:    init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_init(hid_t vipl_id)
{
    auto log = get_logger();

    log->debug("------- PASS THROUGH VOL INIT");
    log->trace("_init(), info = {}\n", fmt::ptr(info));

    return 0;
}

/*---------------------------------------------------------------------------
 * Function:    term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_term(void)
{
    auto log = get_logger();

    log->debug("------- PASS THROUGH VOL TERM");

    herr_t result = 0;

    // Reset VOL ID
    connector_id = H5I_INVALID_HID;     // NB: this is the only reason connector_id is static

    delete info_t::vol;
    info_t::vol = nullptr;
    auto* configuration = info;
    info = nullptr;
    _info_free(configuration);

    return result;
}

/*-------------------------------------------------------------------------
 * Function:    optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL generic Optional");

    ret_value = o->vol->optional(o->under_object, args, dxpl_id, req);
    if (req && *req)
        *req = o->create(*req);

    return ret_value;
}

herr_t
LowFive::VOLBase::
optional(void *obj, H5VL_optional_args_t* args, hid_t dxpl_id, void **req)
{
    return H5VLoptional(obj, info->under_vol_id, args, dxpl_id, req);
}
