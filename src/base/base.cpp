#include <lowfive/vol-base.hpp>

hid_t LowFive::VOLBase::connector_id = -1;

LowFive::VOLBase::
VOLBase(unsigned version_, int value_, std::string name_):
    version(version_), value(value_), name(name_)
{
    connector =
    {
        version,                                        /* version      */
        (H5VL_class_value_t) value,                     /* value        */
        name.c_str(),                                   /* name         */
        0,                                              /* capability flags */
        &_init,                                         /* initialize   */
        &_term,                                         /* terminate    */
        {                                               /* info_cls */
            sizeof(info_t),                             /* size    */
            &_info_copy,                                /* copy    */
            NULL, //&OUR_pass_through_info_cmp,                  /* compare */
            &_info_free,                                /* free    */
            NULL, //&OUR_pass_through_info_to_str,               /* to_str  */
            NULL  //&OUR_pass_through_str_to_info,               /* from_str */
        },
        {                                           /* wrap_cls */
            NULL, // OUR_pass_through_get_object,               /* get_object   */
            NULL, // OUR_pass_through_get_wrap_ctx,             /* get_wrap_ctx */
            NULL, // OUR_pass_through_wrap_object,              /* wrap_object  */
            NULL, // OUR_pass_through_unwrap_object,            /* unwrap_object */
            NULL  // OUR_pass_through_free_wrap_ctx,            /* free_wrap_ctx */
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
            NULL, // OUR_pass_through_dataset_specific,         /* specific */
            NULL, // OUR_pass_through_dataset_optional,         /* optional */
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
            NULL, // OUR_pass_through_file_specific,            /* specific */
            &_file_optional,                             /* optional */
            &_file_close                                 /* close */
        },
        {                                           /* group_cls */
            &_group_create,                                     /* create */
            NULL, // OUR_pass_through_group_open,               /* open */
            NULL, // OUR_pass_through_group_get,                /* get */
            NULL, // OUR_pass_through_group_specific,           /* specific */
            NULL, // OUR_pass_through_group_optional,           /* optional */
            &_group_close                                       /* close */
        },
        {                                           /* link_cls */
            NULL, // OUR_pass_through_link_create,              /* create */
            NULL, // OUR_pass_through_link_copy,                /* copy */
            NULL, // OUR_pass_through_link_move,                /* move */
            NULL, // OUR_pass_through_link_get,                 /* get */
            NULL, // OUR_pass_through_link_specific,            /* specific */
            NULL  // OUR_pass_through_link_optional,            /* optional */
        },
        {                                           /* object_cls */
            NULL, // OUR_pass_through_object_open,              /* open */
            NULL, // OUR_pass_through_object_copy,              /* copy */
            NULL, // OUR_pass_through_object_get,               /* get */
            NULL, // OUR_pass_through_object_specific,          /* specific */
            NULL  // OUR_pass_through_object_optional,          /* optional */
        },
        {                                           /* introspect_cls */
            &_introspect_get_conn_cls,                          /* get_conn_cls */
            &_introspect_opt_query,                             /* opt_query */
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
            NULL, // OUR_pass_through_blob_put,                 /* put */
            NULL, // OUR_pass_through_blob_get,                 /* get */
            NULL, // OUR_pass_through_blob_specific,            /* specific */
            NULL, // OUR_pass_through_blob_optional             /* optional */
        },
        {                                           /* token_cls */
            NULL, // OUR_pass_through_token_cmp,                /* cmp */
            NULL, // OUR_pass_through_token_to_str,             /* to_str */
            NULL, // OUR_pass_through_token_from_str              /* from_str */
        },
        NULL // OUR_pass_through_optional                  /* optional */
    };
    info.vol = this;
}

hid_t
LowFive::VOLBase::
register_plugin()
{
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
    printf("------- PASS THROUGH VOL INIT\n");
#endif

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
