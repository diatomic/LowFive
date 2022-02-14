#include <lowfive/vol-base.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>

hid_t LowFive::VOLBase::connector_id = -1;

H5VL_class_t LowFive::VOLBase::connector =
{
    0,                                              /* version      */
    (H5VL_class_value_t) 510,                       /* value        */
    "lowfive",                                      /* name         */
    0,                                              /* capability flags */
    &_init,                                         /* initialize   */
    &_term,                                         /* terminate    */
    {                                               /* info_cls */
        sizeof(info_t),                             /* size    */
        &_info_copy,                                /* copy    */
        NULL, //&OUR_pass_through_info_cmp,                  /* compare */
        &_info_free,                                /* free    */
        &_info_to_str,                              /* to_str  */
        &_str_to_info,                              /* from_str */
    },
    {                                           /* wrap_cls */
        &_wrap_get_object,                          /* get_object   */
        &_get_wrap_ctx,                             /* get_wrap_ctx */
        &_wrap_object,                              /* wrap_object  */
        &_unwrap_object,                            /* unwrap_object */
        &_free_wrap_ctx,                            /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        &_attr_create,                              /* create */
        &_attr_open,                                /* open */
        &_attr_read,                                /* read */
        &_attr_write,                               /* write */
        &_attr_get,                                 /* get */
        &_attr_specific,                            /* specific */
        &_attr_optional,                            /* optional */
        &_attr_close                                /* close */
    },
    {                                           /* dataset_cls */
        &_dataset_create,                           /* create */
        &_dataset_open,                             /* open */
        &_dataset_read,                             /* read */
        &_dataset_write,                            /* write */
        &_dataset_get,                              /* get */
        &_dataset_specific,                         /* specific */
        &_dataset_optional,                         /* optional */
        &_dataset_close                             /* close */
    },
    {                                           /* datatype_cls */
        NULL, // OUR_pass_through_datatype_commit,          /* commit */
        NULL, // OUR_pass_through_datatype_open,            /* open */
        NULL, // OUR_pass_through_datatype_get,             /* get_size */
        NULL, // OUR_pass_through_datatype_specific,        /* specific */
        NULL, // OUR_pass_through_datatype_optional,        /* optional */
        NULL  // OUR_pass_through_datatype_close            /* close */
    },
    {                                           /* file_cls */
        &_file_create,                               /* create */
        &_file_open,                                 /* open */
        &_file_get,                                  /* get */
        &_file_specific,                             /* specific */
        &_file_optional,                             /* optional */
        &_file_close                                 /* close */
    },
    {                                           /* group_cls */
        &_group_create,                              /* create */
        &_group_open,                                /* open */
        NULL, // OUR_pass_through_group_get,                /* get */
        NULL, // OUR_pass_through_group_specific,           /* specific */
        NULL, // OUR_pass_through_group_optional,           /* optional */
        &_group_close                                       /* close */
    },
    {                                           /* link_cls */
        &_link_create,                              /* create */
        &_link_copy,                                /* copy */
        &_link_move,                                /* move */
        &_link_get,                                 /* get */
        &_link_specific,                            /* specific */
        &_link_optional                             /* optional */
    },
    {                                           /* object_cls */
        NULL, // OUR_pass_through_object_open,              /* open */
        NULL, // OUR_pass_through_object_copy,              /* copy */
        &_object_get,                                /* get */
        &_object_specific,                           /* specific */
        NULL  // OUR_pass_through_object_optional,          /* optional */
    },
    {                                           /* introspect_cls */
        &_introspect_get_conn_cls,                   /* get_conn_cls */
        &_introspect_opt_query,                      /* opt_query */
    },
    {                                           /* request_cls */
        NULL, // OUR_pass_through_request_wait,             /* wait */
        NULL, // OUR_pass_through_request_notify,           /* notify */
        NULL, // OUR_pass_through_request_cancel,           /* cancel */
        NULL, // OUR_pass_through_request_specific,         /* specific */
        NULL, // OUR_pass_through_request_optional,         /* optional */
        NULL  // OUR_pass_through_request_free              /* free */
    },
    {                                           /* blob_cls */
        &_blob_put,                                  /* put */
        NULL, // OUR_pass_through_blob_get,                 /* get */
        NULL, // OUR_pass_through_blob_specific,            /* specific */
        NULL, // OUR_pass_through_blob_optional             /* optional */
    },
    {                                           /* token_cls */
        &_token_cmp,                                 /* cmp */
        NULL, // OUR_pass_through_token_to_str,             /* to_str */
        NULL, // OUR_pass_through_token_from_str              /* from_str */
    },
    NULL // OUR_pass_through_optional                  /* optional */
};

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_VOL; }
const void *H5PLget_plugin_info(void)
{
    fmt::print(stderr, "H5PLget_plugin_info\n");
    return &LowFive::VOLBase::connector;
}

LowFive::VOLBase::
VOLBase()
{
    fmt::print(stderr, "VOLBase::VOLBase(), &info = {}\n", fmt::ptr(&info));

    // this is here to trigger HDF5 initialization, in case this constructor is
    // called before anything else; it would be nice to find a cleaner way to
    // do this, but this works for now
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    H5Pclose(plist);

    if (info == NULL)
    {
        VOLBase::info = new info_t;
        VOLBase::info->under_vol_id = H5VL_NATIVE;      // NB: H5VL_NATIVE is not a variable, it's a macro that calls a function!
        VOLBase::info->under_vol_info = NULL;
    }

    fmt::print(stderr, "VOLBase::VOLBase()\n");
    info_t::vol = this;
}

LowFive::VOLBase::
~VOLBase()
{
    // TODO: detele info
}

hid_t
LowFive::VOLBase::
register_plugin()
{
    fmt::print(stderr, "registering plugin, info = {}\n", fmt::ptr(info));

    // Singleton register the pass-through VOL connector ID
    if (connector_id < 0)
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
#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    fprintf(stderr, "------- PASS THROUGH VOL INIT\n");
#endif
    fmt::print(stderr, "_init(), info = {}\n", fmt::ptr(info));

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
#ifdef LOWFIVE_ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL TERM\n");
#endif

    herr_t result = 0;

    // Reset VOL ID
    connector_id = H5I_INVALID_HID;     // NB: this is the only reason connector_id is static

    return result;
}
