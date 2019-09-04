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
        &init,                                          /* initialize   */
        &term,                                          /* terminate    */
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
            NULL, // OUR_pass_through_dataset_read,             /* read */
            NULL, // OUR_pass_through_dataset_write,            /* write */
            NULL, // OUR_pass_through_dataset_get,              /* get */
            NULL, // OUR_pass_through_dataset_specific,         /* specific */
            NULL, // OUR_pass_through_dataset_optional,         /* optional */
            NULL  // OUR_pass_through_dataset_close             /* close */
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
            NULL, // OUR_pass_through_file_get,                 /* get */
            NULL, // OUR_pass_through_file_specific,            /* specific */
            NULL, // OUR_pass_through_file_optional,            /* optional */
            &_file_close                                 /* close */
        },
        {                                           /* group_cls */
            NULL, // OUR_pass_through_group_create,             /* create */
            NULL, // OUR_pass_through_group_open,               /* open */
            NULL, // OUR_pass_through_group_get,                /* get */
            NULL, // OUR_pass_through_group_specific,           /* specific */
            NULL, // OUR_pass_through_group_optional,           /* optional */
            NULL  // OUR_pass_through_group_close               /* close */
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
        {                                           /* request_cls */
            NULL, // OUR_pass_through_request_wait,             /* wait */
            NULL, // OUR_pass_through_request_notify,           /* notify */
            NULL, // OUR_pass_through_request_cancel,           /* cancel */
            NULL, // OUR_pass_through_request_specific,         /* specific */
            NULL, // OUR_pass_through_request_optional,         /* optional */
            NULL  // OUR_pass_through_request_free              /* free */
        },
        NULL                                        /* optional */
    };
    info.vol_derived = this;
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
 * Function:    new_obj
 *
 * Purpose:     Create a new pass through object for an underlying object
 *
 * Return:      Success:    Pointer to the new pass through object
 *              Failure:    NULL
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
typename VOLBase<Derived>::pass_through_t*
VOLBase<Derived>::
new_obj(void *under_obj, hid_t under_vol_id, void* vol_derived)
{
    pass_through_t *new_obj;

    new_obj = (pass_through_t *)calloc(1, sizeof(pass_through_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    new_obj->vol_derived  = vol_derived;
    H5Iinc_ref(new_obj->under_vol_id);

    return new_obj;
} /* end new_obj() */

/*-------------------------------------------------------------------------
 * Function:    free_obj
 *
 * Purpose:     Release a pass through object
 *
 * Note:        Take care to preserve the current HDF5 error stack
 *              when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
template<class Derived>
herr_t
VOLBase<Derived>::
free_obj(pass_through_t *obj)
{
    hid_t err_id;

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);

    return 0;
} /* end free_obj() */


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
init(hid_t vipl_id)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL INIT\n");
#endif

    if (&Derived::init != &VOLBase<Derived>::init)
        return Derived::init(vipl_id);
    else
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
template<class Derived>
herr_t
VOLBase<Derived>::
term(void)
{
#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL TERM\n");
#endif

    herr_t result = 0;
    if (&Derived::term != &VOLBase<Derived>::term)
        result = Derived::term();

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
    static_cast<Derived*>(info->vol_derived)->info_copy(_info);

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

    void* result = static_cast<Derived*>(o->vol_derived)->dataset_create(obj, loc_params, name, lcpl_id, type_id, space_id, dcpl_id, dapl_id, dxpl_id, req);
    // TODO: need a mechanism to skip the following code, depending on the result

    under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);
    if(under) {
        dset = new_obj(under, o->under_vol_id, o->vol_derived);

        /* Check for async request */
        if(req && *req)
            *req = new_obj(*req, o->under_vol_id, o->vol_derived);
    } /* end if */
    else
        dset = NULL;

    return (void *)dset;
} /* end dataset_create() */


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
        file = new_obj(under, info->under_vol_id, info->vol_derived);

        /* Check for async request */
        if(req && *req)
            *req = new_obj(*req, info->under_vol_id, info->vol_derived);
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
        file = new_obj(under, info->under_vol_id, info->vol_derived);

        /* Check for async request */
        if(req && *req)
            *req = new_obj(*req, info->under_vol_id, info->vol_derived);
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
        *req = new_obj(*req, o->under_vol_id, o->vol_derived);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0)
        free_obj(o);

    return ret_value;
} /* end file_close() */
