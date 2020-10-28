template<class Derived>
hid_t VOLBase<Derived>::connector_id = -1;

template<class Derived>
VOLBase<Derived>::
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
            NULL, // OUR_pass_through_attr_create,              /* create */
            NULL, // OUR_pass_through_attr_open,                /* open */
            NULL, // OUR_pass_through_attr_read,                /* read */
            NULL, // OUR_pass_through_attr_write,               /* write */
            NULL, // OUR_pass_through_attr_get,                 /* get */
            NULL, // OUR_pass_through_attr_specific,            /* specific */
            NULL, // OUR_pass_through_attr_optional,            /* optional */
            NULL  // OUR_pass_through_attr_close                /* close */
        },
        {                                               /* dataset_cls */
            &_dataset_create,                            /* create */
            NULL, // OUR_pass_through_dataset_open,             /* open */
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
            NULL, // OUR_pass_through_introspect_get_conn_cls,  /* get_conn_cls */
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
    info.vol = static_cast<Derived*>(this);
}

template<class Derived>
hid_t
VOLBase<Derived>::
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
template<class Derived>
herr_t
VOLBase<Derived>::
_init(hid_t vipl_id)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INIT\n");
#endif

    return Derived::init(vipl_id);
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
template<class Derived>
herr_t
VOLBase<Derived>::
_term(void)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL TERM\n");
#endif

    herr_t result = Derived::term();

    // Reset VOL ID
    connector_id = H5I_INVALID_HID;     // NB: this is the only reason connector_id is static

    return result;
}

/*---------------------------------------------------------------------------
 * Function:    info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
template<class Derived>
void *
VOLBase<Derived>::
_info_copy(const void *_info)
{
    const info_t *info = (const info_t *)_info;
    info_t *new_info;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the pass through connector */
    new_info = (info_t *)calloc(1, sizeof(info_t));

    // XXX: this is currently pointless; think of a situation, where this could be useful
    info->vol->info_copy(_info);

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if(info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return new_info;
} /* end info_copy() */

/*---------------------------------------------------------------------------
 * Function:    info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:        Take care to preserve the current HDF5 error stack
 *              when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_info_free(void *_info)
{
    info_t *info = (info_t *)_info;
    hid_t err_id;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through info object itself */
    free(info);

    return 0;
} /* end info_free() */

/*-------------------------------------------------------------------------
 * Function:    dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
void*
VOLBase<Derived>::
_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    pass_through_t *dset;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL DATASET Create\n");
#endif

    void* result = o->vol->dataset_create(obj, loc_params, name, lcpl_id, type_id, space_id, dcpl_id, dapl_id, dxpl_id, req);
    // TODO: need a mechanism to skip the following code, depending on the result

    under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);
    if(under) {
        dset = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        dset = NULL;

    return (void *)dset;
} /* end dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    OUR_pass_through_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL DATASET Read\n");
#endif

    ret_value = o->vol->dataset_read(dset, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    // TODO: need a mechanism to skip the following code, depending on the result

    ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    OUR_pass_through_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL DATASET Write\n");
#endif

    ret_value = o->vol->dataset_write(dset, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    // TODO: need a mechanism to skip the following code, depending on the result

    ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end dataset_write() */

/*-------------------------------------------------------------------------
 * Function:    dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL DATASET Get\n");
#endif

    ret_value = o->vol->dataset_get(dset, get_type, dxpl_id, req, arguments);
    // TODO: need a mechanism to skip the following code, depending on the result

    ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end pass_through_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL DATASET Close\n");
#endif

    ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying dataset was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end dataset_close() */

/*-------------------------------------------------------------------------
 * Function:    file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
void*
VOLBase<Derived>::
_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Create\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    if(under) {
        file = new pass_through_t(under, info->under_vol_id, info->vol);

        /* Check for async request */
        if(req && *req)
            *req = new pass_through_t(*req, info->under_vol_id, info->vol);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    _info_free(info);

    return (void *)file;
} /* end file_create() */

/*-------------------------------------------------------------------------
 * Function:    file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
void*
VOLBase<Derived>::
_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    info_t *info;
    pass_through_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
    if(under) {
        file = new pass_through_t(under, info->under_vol_id, info->vol);

        /* Check for async request */
        if(req && *req)
            *req = new pass_through_t(*req, info->under_vol_id, info->vol);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    _info_free(info);

    return (void *)file;
} /* end file_open() */

/*-------------------------------------------------------------------------
 * Function:    file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
    void **req, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL FILE Get\n");
#endif

    ret_value = H5VLfile_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_get() */

/*-------------------------------------------------------------------------
 * Function:    file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_file_optional(void *file, H5VL_file_optional_t opt_type,
    hid_t dxpl_id, void **req, va_list arguments)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL File Optional\n");
#endif

    ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, opt_type, dxpl_id, req, arguments);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end file_optional() */

/*-------------------------------------------------------------------------
 * Function:    file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_file_close(void *file, hid_t dxpl_id, void **req)
{
    pass_through_t *o = (pass_through_t *)file;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL FILE Close\n");
#endif

    ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end file_close() */

/*-------------------------------------------------------------------------
 * Function:    group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
void*
VOLBase<Derived>::
_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
    hid_t dxpl_id, void **req)
{
    pass_through_t *group;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL GROUP Create\n");
#endif

    void* result = o->vol->group_create(obj, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
    // TODO: do something with the result

    under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);
    if(under) {
        group = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        group = NULL;

    return (void *)group;
} /* end group_create() */

/*-------------------------------------------------------------------------
 * Function:    group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_group_close(void *grp, hid_t dxpl_id, void **req)
{
    pass_through_t *o = (pass_through_t *)grp;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING 
    printf("------- PASS THROUGH VOL H5Gclose\n");
#endif

    ret_value = o->vol->group_close(grp, dxpl_id, req);
    // TODO: do something with ret_value

    ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end group_close() */

/*-------------------------------------------------------------------------
 * Function:    introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if(H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = o->vol->connector;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id,
            lvl, conn_cls);

    return ret_value;
} /* end introspect_get_conn_cls() */


/*-------------------------------------------------------------------------
 * Function:    introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported)
{
    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INTROSPECT OptQuery\n");
#endif

    ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls,
        opt_type, supported);

    return ret_value;
} /* end introspect_opt_query() */


