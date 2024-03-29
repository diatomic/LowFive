#include <lowfive/vol-base.hpp>
#include "../log-private.hpp"

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
void*
LowFive::VOLBase::
_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
        const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
        hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *dset;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL DATASET Create");

    under = o->vol->dataset_create(o->under_object, loc_params, name, lcpl_id, type_id, space_id, dcpl_id, dapl_id, dxpl_id, req);

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

void*
LowFive::VOLBase::
dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
        const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
        hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    return H5VLdataset_create(obj, loc_params, info->under_vol_id, name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void*
LowFive::VOLBase::
_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
        const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    CALI_CXX_MARK_FUNCTION;
    auto log = get_logger();

    pass_through_t *dset;
    pass_through_t *o = (pass_through_t *)obj;
    void *under;

    log->debug("------- PASS THROUGH VOL DATASET Open");

    under = o->vol->dataset_open(o->under_object, loc_params, name, dapl_id, dxpl_id, req);

    if(under) {
        dset = o->create(under);

        /* Check for async request */
        if(req && *req)
            *req = o->create(*req);
    } /* end if */
    else
        dset = NULL;

    return (void *)dset;
} /* end dataset_open() */

void*
LowFive::VOLBase::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
        const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    return H5VLdataset_open(obj, loc_params, info->under_vol_id, name, dapl_id, dxpl_id, req);
}


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
herr_t
LowFive::VOLBase::
_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATASET Close");

    ret_value = o->vol->dataset_close(o->under_object, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    /* Release our wrapper, if underlying dataset was closed */
    if(ret_value >= 0)
        pass_through_t::destroy(o);

    return ret_value;
} /* end dataset_close() */

herr_t
LowFive::VOLBase::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    return H5VLdataset_close(dset, info->under_vol_id, dxpl_id, req);
}

herr_t
LowFive::VOLBase::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    return H5VLdataset_read(1, &dset, info->under_vol_id, &mem_type_id, &mem_space_id, &file_space_id, plist_id, &buf, req);
}

herr_t
LowFive::VOLBase::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    return H5VLdataset_write(1, &dset, info->under_vol_id, &mem_type_id, &mem_space_id, &file_space_id, plist_id, &buf, req);
}

/*-------------------------------------------------------------------------
 * Function:    dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_dataset_read(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[], hid_t plist_id, void *buf[], void **req)
{
    auto log = get_logger();

    log->debug("------- PASS THROUGH VOL DATASET Read count = {}", count);

    herr_t ret_value = 0;

    // iterate over all datasets
    for(size_t dset_idx = 0; dset_idx < count; ++ dset_idx) {

        pass_through_t* o = (pass_through_t*)dset[dset_idx];

        // call dataset_read
        auto ret_value_per_dset = o->vol->dataset_read(o->under_object, mem_type_id[dset_idx], mem_space_id[dset_idx], file_space_id[dset_idx], plist_id, buf[dset_idx], req);

        if (ret_value == 0) {
            ret_value = ret_value_per_dset;
            // TODO: signal error and break loop?
            // Is anything really going to call it with count > 1?
        }

        /* Check for async request */
        if (req && *req)
            *req = o->create(*req);
    }

    return ret_value;
} /* end dataset_read() */



/*-------------------------------------------------------------------------
 * Function:    dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_dataset_write(size_t count, void *dset[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[], hid_t plist_id, const void *buf[], void **req)
{
    auto log = get_logger();

    herr_t ret_value = 0;

    log->debug("------- PASS THROUGH VOL DATASET Write count = {}", count);
    for(size_t dset_idx = 0; dset_idx < count; ++dset_idx) {

        pass_through_t* o = (pass_through_t*)dset[dset_idx];

        auto ret_value_loc = o->vol->dataset_write(o->under_object, mem_type_id[dset_idx], mem_space_id[dset_idx], file_space_id[dset_idx], plist_id, buf[dset_idx], req);

        if (ret_value == 0) {
            // do not overwrite FAIL
            // TODO: break here if ret_value_loc == FAIL?
            ret_value = ret_value_loc;
        }

        /* Check for async request */
        if (req && *req)
            *req = o->create(*req);
    }

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
herr_t
LowFive::VOLBase::
_dataset_get(void *dset, H5VL_dataset_get_args_t* args, hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)dset;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATASET Get");

    ret_value = o->vol->dataset_get(o->under_object, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* end pass_through_dataset_get() */

herr_t
LowFive::VOLBase::
dataset_get(void *dset, H5VL_dataset_get_args_t* args, hid_t dxpl_id, void **req)
{
    return H5VLdataset_get(dset, info->under_vol_id, args, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    dataset_specific
 *
 * Purpose:     Specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_dataset_specific(void *obj, H5VL_dataset_specific_args_t* args,
        hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    //hid_t under_vol_id;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATASET Specific");

    // Save copy of underlying VOL connector ID and prov helper, in case of
    // refresh destroying the current object
    //under_vol_id = o->under_vol_id;

    ret_value = o->vol->dataset_specific(o->under_object, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* dataset_specific() */

herr_t
LowFive::VOLBase::
dataset_specific(void *obj, H5VL_dataset_specific_args_t* args,
        hid_t dxpl_id, void **req)
{
    return H5VLdataset_specific(obj, info->under_vol_id, args, dxpl_id, req);
}

/*-------------------------------------------------------------------------
 * Function:    dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
LowFive::VOLBase::
_dataset_optional(void *obj, H5VL_optional_args_t* args,
        hid_t dxpl_id, void **req)
{
    auto log = get_logger();

    pass_through_t *o = (pass_through_t *)obj;
    herr_t ret_value;

    log->debug("------- PASS THROUGH VOL DATASET Optional");

    ret_value = o->vol->dataset_optional(o->under_object, args, dxpl_id, req);

    /* Check for async request */
    if(req && *req)
        *req = o->create(*req);

    return ret_value;
} /* dataset_optional() */

herr_t
LowFive::VOLBase::
dataset_optional(void *obj, H5VL_optional_args_t* args,
        hid_t dxpl_id, void **req)
{
    return H5VLdataset_optional(obj, info->under_vol_id, args, dxpl_id, req);
}