#include <lowfive/vol-metadata.hpp>

void*
LowFive::MetadataVOL::
file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ptrs = nullptr;

    void* mdata = nullptr;
    if (vol_properties.memory)
    {
        // create our file metadata
        std::string name_(name);
        File* f = new File(name_);
        files.emplace(name_, f);
        mdata = f;
    }

    if (vol_properties.passthru)
        obj_ptrs = (ObjectPointers*) VOLBase::file_create(name, flags, fcpl_id, fapl_id, dxpl_id, req);
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    fmt::print("file_create: obj_ptrs = {} [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(obj_ptrs), fmt::ptr(obj_ptrs->h5_obj), fmt::ptr(obj_ptrs->mdata_obj), dxpl_id);

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    fmt::print(stderr, "file_optional: opt_type = {}\n", opt_type);
    // the meaning of opt_type is defined in H5VLnative.h (H5VL_NATIVE_FILE_* constants)

    herr_t res = 0;
    if (vol_properties.passthru && unwrap(file_))
        res = VOLBase::file_optional(file_, opt_type, dxpl_id, req, arguments);

    return res;
}

void*
LowFive::MetadataVOL::
file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ptrs = nullptr;

    void* mdata = nullptr;
    if (vol_properties.memory)
    {
        // create our file metadata
        std::string name_(name);
        File* f = new File(name_);
        files.emplace(name_, f);
        mdata = f;
    }

    if (vol_properties.passthru && ! vol_properties.memory)
        obj_ptrs = (ObjectPointers*) VOLBase::file_open(name, flags, fapl_id, dxpl_id, req);
    else
        obj_ptrs = new ObjectPointers;

    obj_ptrs->mdata_obj = mdata;

    return obj_ptrs;
}

herr_t
LowFive::MetadataVOL::
file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    va_list args;
    va_copy(args,arguments);

    fmt::print("file_get: file = {} [h5_obj {} mdata_obj {}], get_type = {} req = {} dxpl_id = {}\n",
            fmt::ptr(file_), fmt::ptr(file_->h5_obj), fmt::ptr(file_->mdata_obj), get_type, fmt::ptr(req), dxpl_id);
    // enum H5VL_file_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::file_get(file_, get_type, dxpl_id, req, arguments);

    // else: TODO

    return result;
}

herr_t
LowFive::MetadataVOL::
file_close(void *file, hid_t dxpl_id, void **req)
{
    ObjectPointers* file_ = (ObjectPointers*) file;

    herr_t res = 0;

    if (vol_properties.passthru && unwrap(file_))
        res = VOLBase::file_close(file_, dxpl_id, req);

    // deliberately verbose, to emphasize checking of res
    if (res == 0)
        return 0;
    else
        return res;
}


void*
LowFive::MetadataVOL::
dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
               const char *name,
               hid_t lcpl_id,  hid_t type_id,
               hid_t space_id, hid_t dcpl_id,
               hid_t dapl_id,  hid_t dxpl_id, void **req)
{
//     fmt::print("loc type = {}, name = {}\n", loc_params->type, name);
//     fmt::print("data type = {}\n", Datatype(type_id));
//     fmt::print("data space = {}\n", Dataspace(space_id));

    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "Dataset Create\n");
    fmt::print("dataset_create: parent obj = {} [h5_obj {} mdata_obj {}], name = {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), name);

    if (vol_properties.passthru)
        result = (ObjectPointers*) VOLBase::dataset_create(obj_, loc_params, name, lcpl_id,  type_id, space_id, dcpl_id, dapl_id,  dxpl_id, req);
    else
        result = new ObjectPointers;

    if (vol_properties.memory)                                              // add the dataset to our file metadata
    {
        // trace object back to root to build full path and file name
        std::string name_(name);
        std::string full_path;
        std::string filename;
        backtrack_name(name_, static_cast<Object*>(obj_->mdata_obj), filename, full_path);

        // check the ownership map for the full path name and file name
        Dataset::Ownership own = Dataset::Ownership::user;
        for (auto& o : data_owners)
        {
            // o.filename and o.full_path can have wildcards '*' and '?'
            if (match(o.filename.c_str(), filename.c_str()) && match(o.full_path.c_str(), full_path.c_str()))
            {
                own = o.ownership;
                break;
            }
        }

        // add the dataset
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Dataset(name_, type_id, space_id, own));

    }
    fmt::print(stderr, "dataset_create: created result {} result->h5_obj {} result->mdata_obj {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    if (vol_properties.passthru && !vol_properties.memory)
        result = (ObjectPointers*) VOLBase::dataset_open(obj_, loc_params, name, dapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the dataset in our file metadata
    std::string name_(name);
    if (vol_properties.memory)
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    va_list args;
    va_copy(args,arguments);

    fmt::print("dset = {}, get_type = {}, req = {}\n", fmt::ptr(unwrap(dset_)), get_type, fmt::ptr(req));
    // enum H5VL_dataset_get_t is defined in H5VLconnector.h and lists the meaning of the values

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::dataset_get(dset_, get_type, dxpl_id, req, arguments);

    if (vol_properties.memory)
    {
        if (get_type == H5VL_DATASET_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Dataset*>(dset_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_DATASET_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Dataset*>(dset_->mdata_obj)->type;

            fmt::print("dataset data type id = {}, datatype = {}\n",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            fmt::print("copied data type id = {}, datatype = {}\n",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Warning, unknown get_type == {} in dataset_get()", get_type));
        }
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print("dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}\n",
               fmt::ptr(unwrap(dset_)),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (vol_properties.memory)
    {
        Dataset* ds = (Dataset*) dset_->mdata_obj;              // dataset from our metadata

        // sanity check that the datatype and dimensionality being read matches the metadata
        // TODO: HDF5 allows datatypes to not match and takes care of the conversion;
        //       eventually, we need to support this functionality as well
        if (ds->type.dtype_class != convert_type_class(H5Tget_class(mem_type_id)) ||
                ds->type.dtype_size != H5Tget_size(mem_type_id))
            throw MetadataError(fmt::format("Error: dataset_read(): type mismatch"));
        if (Dataspace(file_space_id).dim != ds->space.dim)
            throw MetadataError(fmt::format("Error: dataset_read(): dim mismatch"));

        for (auto& dt : ds->data)                               // for all the data triples in the metadata dataset
        {
            Dataspace& fs = dt.file;

            hid_t mem_dst = Dataspace::project_intersection(file_space_id, mem_space_id, fs.id);
            hid_t mem_src = Dataspace::project_intersection(fs.id,         dt.memory.id, file_space_id);

            Dataspace dst(mem_dst, true);
            Dataspace src(mem_src, true);

            Dataspace::iterate(dst, Datatype(mem_type_id).dtype_size, src, ds->type.dtype_size, [&](size_t loc1, size_t loc2, size_t len)
                    {
                    std::memcpy((char*) buf + loc1, (char*) dt.data + loc2, len);
                    });

            fmt::print("dst = {}\n", dst);
            fmt::print("src = {}\n", src);
        }   // for all data triples
    }

    if (vol_properties.passthru)
        return VOLBase::dataset_read(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print(stderr, "Dataset Write\n");
    fmt::print("dset = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})\n",
               fmt::ptr(unwrap(dset_)),
               mem_space_id, Dataspace(mem_space_id),
               file_space_id, Dataspace(file_space_id));

    if (vol_properties.memory)
    {
        // save our metadata
        Dataset* ds = (Dataset*) dset_->mdata_obj;
        ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);
    }

    if (vol_properties.passthru)
        return VOLBase::dataset_write(dset_, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

    fmt::print(stderr, "Dataset Close\n");
    fmt::print(stderr, "dataset_close: dset = {} = [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(dset_), fmt::ptr(dset_->h5_obj), fmt::ptr(dset_->mdata_obj), dxpl_id);
    if (dset_->mdata_obj)
        fmt::print(stderr, "closing dataset name {}\n", static_cast<Object*>(dset_->mdata_obj)->name);

    herr_t retval = 0;

    // close from the file if not using metadata in memory
    // if both memory and passthru are enabled, close from file (producer) only
    if (vol_properties.passthru && !vol_properties.memory)
        retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    else if (vol_properties.passthru && vol_properties.memory)
    {
        if (Dataset* ds = dynamic_cast<Dataset*>((Object*) dset_->mdata_obj))
            retval = VOLBase::dataset_close(dset_, dxpl_id, req);
    }

    return retval;
}

void*
LowFive::MetadataVOL::
group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Group Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    ObjectPointers* result = nullptr;
    if (vol_properties.passthru)
        result = (ObjectPointers*) VOLBase::group_create(obj_, loc_params, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // add group to our metadata
    if (vol_properties.memory)
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Group(name));

    return (void*)result;
}

void*
LowFive::MetadataVOL::
group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    if (vol_properties.passthru && !vol_properties.memory)
        result = (ObjectPointers*) VOLBase::group_open(obj_, loc_params, name, gapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the group in our file metadata
    std::string name_(name);
    if (vol_properties.memory)
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
group_close(void *grp, hid_t dxpl_id, void **req)
{
    ObjectPointers* grp_ = (ObjectPointers*) grp;

    fmt::print(stderr, "Group Close\n");

    herr_t retval = 0;

    if (vol_properties.passthru)
        retval = VOLBase::group_close(grp_, dxpl_id, req);

    // TODO: why create a pointer that isn't used?
    if (vol_properties.memory)
        Group* g = (Group*) grp_->mdata_obj;

    return retval;
}

void*
LowFive::MetadataVOL::
attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    fmt::print(stderr, "Attr Create\n");
    fmt::print("loc type = {}, name = {}\n", loc_params->type, name);

    ObjectPointers* result = nullptr;
    if (vol_properties.passthru)
        result = (ObjectPointers*) VOLBase::attr_create(obj_, loc_params, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // add attribute to our metadata
    if (vol_properties.memory)
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->add_child(new Attribute(name, type_id, space_id));

    fmt::print("created attribute object = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return result;
}

void*
LowFive::MetadataVOL::
attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;
    ObjectPointers* result = nullptr;

    fmt::print(stderr, "Attr Open\n");
    fmt::print("attr_open obj = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), name);

    // open from the file if not opening from memory
    // if both memory and passthru are enabled, open from memory only
    if (vol_properties.passthru && !vol_properties.memory)
        result = (ObjectPointers*) VOLBase::attr_open(obj_, loc_params, name, aapl_id, dxpl_id, req);
    else
        result = new ObjectPointers;

    // find the attribute in our file metadata
    std::string name_(name);
    if (vol_properties.memory)
        result->mdata_obj = static_cast<Object*>(obj_->mdata_obj)->search(name_);

    fmt::print("attr_open search result = {} = [h5_obj {} mdata_obj {}] name {}\n",
            fmt::ptr(result), fmt::ptr(result->h5_obj), fmt::ptr(result->mdata_obj), name);

    return (void*)result;
}

herr_t
LowFive::MetadataVOL::
attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr = {}, get_type = {}, req = {}\n", fmt::ptr(unwrap(obj_)), get_type, fmt::ptr(req));

    fmt::print(stderr, "Attr Get\n");
    fmt::print("get type = {}\n", get_type);

    if (vol_properties.passthru)
    {
        return VOLBase::attr_get(obj, get_type, dxpl_id, req, arguments);
    }
    else if (vol_properties.memory)
    {
        if (get_type == H5VL_ATTR_GET_SPACE)
        {
            fmt::print("GET_SPACE\n");
            auto& dataspace = static_cast<Attribute*>(obj_->mdata_obj)->space;

            hid_t space_id = dataspace.copy();
            fmt::print("copied space id = {}, space = {}\n", space_id, Dataspace(space_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = space_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else if (get_type == H5VL_ATTR_GET_TYPE)
        {
            fmt::print("GET_TYPE\n");
            auto& datatype = static_cast<Attribute*>(obj_->mdata_obj)->type;

            fmt::print("dataset data type id = {}, datatype = {}\n",
                    datatype.id, datatype);

            hid_t dtype_id = datatype.copy();
            fmt::print("copied data type id = {}, datatype = {}\n",
                    dtype_id, Datatype(dtype_id));

            hid_t *ret = va_arg(args, hid_t*);
            *ret = dtype_id;
            fmt::print("arguments = {} -> {}\n", fmt::ptr(ret), *ret);
        } else
        {
            throw MetadataError(fmt::format("Warning, unknown get_type == {} in attr_get()", get_type));
        }
    }

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments)
{
    ObjectPointers* obj_ = (ObjectPointers*) obj;

    va_list args;
    va_copy(args,arguments);

    fmt::print("attr_specific obj = {} = [h5_obj {} mdata_obj {}] specific_type = {}\n",
            fmt::ptr(obj_), fmt::ptr(obj_->h5_obj), fmt::ptr(obj_->mdata_obj), specific_type);
    fmt::print("specific types H5VL_ATTR_DELETE = {} H5VL_ATTR_EXISTS = {} H5VL_ATTR_ITER = {} H5VL_ATTR_RENAME = {}\n",
            H5VL_ATTR_DELETE, H5VL_ATTR_EXISTS, H5VL_ATTR_ITER, H5VL_ATTR_RENAME);

    herr_t result = 0;
    if (vol_properties.passthru)
        result = VOLBase::attr_specific(obj_, loc_params, specific_type, dxpl_id, req, arguments);

    else if (vol_properties.memory)
    {
        switch(specific_type)
        {
            case H5VL_ATTR_DELETE:                      // H5Adelete(_by_name/idx)
            {
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_DELETE not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            case H5VL_ATTR_EXISTS:                      // H5Aexists(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_EXISTS\n");

                const char *attr_name = va_arg(arguments, const char *);
                htri_t *    ret       = va_arg(arguments, htri_t *);

                // check direct children of the parent object (NB, not full search all the way down the tree)
                *ret = 0;           // not found
                for (auto& c : static_cast<Object*>(obj_->mdata_obj)->children)
                {
                    if (c->type == LowFive::ObjectType::Attribute && c->name == attr_name)
                    {
                        *ret = 1;   // found
                        break;
                    }
                }
                if (*ret)
                    fmt::print(stderr, "Found attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
                else
                    fmt::print(stderr, "Did not find attribute {} as a child of the parent {}\n", attr_name, static_cast<Object*>(obj_->mdata_obj)->name);
                break;
            }
            case H5VL_ATTR_ITER:                        // H5Aiterate(_by_name)
            {
                fmt::print(stderr, "case H5VL_ATTR_ITER\n");

                // copied from HDF5 H5VLnative_attr.c, line 452
                H5_index_t      idx_type = (H5_index_t)va_arg(arguments, int);
                H5_iter_order_t order    = (H5_iter_order_t)va_arg(arguments, int);
                hsize_t *       idx      = va_arg(arguments, hsize_t *);
                H5A_operator2_t op       = va_arg(arguments, H5A_operator2_t);
                void *          op_data  = va_arg(arguments, void *);

                // check direct children of the parent object (NB, not full search all the way down the tree)
                bool found = false;         // some attributes were found
                for (auto& c : static_cast<Object*>(obj_->mdata_obj)->children)
                {
                    if (c->type == LowFive::ObjectType::Attribute)
                    {
                        found = true;
                        fmt::print(stderr, "Found attribute {} as a child of the parent {}\n",
                                c->name, static_cast<Object*>(obj_->mdata_obj)->name);

                        fmt::print(stderr, "Warning: operating on attribute not implemented yet\n");

//                         // copied from HDF5 H5int.c, line 1785
//                         // Check which type of callback to make
//                         switch (op->op_type)
//                         {
//                             case H5A_ATTR_OP_APP2:
//                             {
//                                 fmt::print(stderr, "op_type = H5A_ATTR_OP_APP2\n");
// 
// //                                 // get the attribute information
// //                                 H5A_info_t ainfo;               // info for attribute
// //                                 if (H5A__get_info(atable->attrs[u], &ainfo) < 0)
// //                                     throw MetadataError(fmt::format("Error: unable to get attribute info.\n");
// // 
// //                                 // make the application callback
// //                                 result = (op->u.app_op2)(loc_id, ((atable->attrs[u])->shared)->name, &ainfo, op_data);
//                                 break;
//                             }
// 
//                             case H5A_ATTR_OP_APP:
//                             {
//                                 fmt::print(stderr, "op_type = H5A_ATTR_OP_APP\n");
// 
// //                                 // make the application callback
// //                                 result = (op->u.app_op)(loc_id, ((atable->attrs[u])->shared)->name, op_data);
//                                 break;
//                             }
// 
//                             case H5A_ATTR_OP_LIB:
//                             {
//                                 fmt::print(stderr, "op_type = H5A_ATTR_OP_LIB\n");
// 
// //                                 // call the library's callback
// //                                 result = (op->u.lib_op)((atable->attrs[u]), op_data);
//                                 break;
//                             }
// 
//                             default:
//                                 throw MetadataError(fmt::format("Error: unsupported attribute op type.\n");
//                         }   // switch on type of callback to make
                    }   // child is type attribute
                }   // for all children
                if (!found)
                {
                    fmt::print(stderr, "Did not find any attributes as direct children of the parent {} when trying to iterate over attributes\n.",
                            static_cast<Object*>(obj_->mdata_obj)->name);
                }
                // TODO
//                 throw MetadataError(fmt::format("Warning, H5VL_ATTR_ITER not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            case H5VL_ATTR_RENAME:                     // H5Arename(_by_name)
            {
//                 fmt::print("Warning, H5VL_ATTR_RENAME not yet implemented in LowFive::MetadataVOL::attr_specific()\n");
                // TODO
                throw MetadataError(fmt::format("Warning, H5VL_ATTR_RENAME not yet implemented in LowFive::MetadataVOL::attr_specific()"));
                break;
            }
            default:
                throw MetadataError(fmt::format("Unknown specific_type in LowFive::MetadataVOL::attr_specific()"));
        }
    }

    return result;
}

herr_t
LowFive::MetadataVOL::
attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print(stderr, "Attr Write\n");
    fmt::print("attr = {} = [h5_obj {} mdata_obj {}], mem_type_id = {}, mem type = {}\n",
            fmt::ptr(attr_), fmt::ptr(attr_->h5_obj), fmt::ptr(attr_->mdata_obj), mem_type_id, Datatype(mem_type_id));

    if (vol_properties.memory)
    {
        // save our metadata
        Attribute* a = (Attribute*) attr_->mdata_obj;
        // skip an attribute that wasn't created or otherwise doesn't have a valide metadata object
//         if (a)
            a->write(Datatype(mem_type_id), buf);
    }

    if (vol_properties.passthru)
        return VOLBase::attr_write(attr_, mem_type_id, buf, dxpl_id, req);

    return 0;
}

herr_t
LowFive::MetadataVOL::
attr_close(void *attr, hid_t dxpl_id, void **req)
{
    ObjectPointers* attr_ = (ObjectPointers*) attr;

    fmt::print(stderr, "Attr Close\n");
    fmt::print("close: attr = {} = [h5_obj {} mdata_obj {}], dxpl_id = {}\n",
            fmt::ptr(attr_), fmt::ptr(attr_->h5_obj), fmt::ptr(attr_->mdata_obj), dxpl_id);

    herr_t retval = 0;

    if (vol_properties.passthru)
        retval = VOLBase::attr_close(attr_, dxpl_id, req);

    // TODO: why create a pointer that isn't used?
    if (vol_properties.memory)
        Attribute* a = (Attribute*) attr_->mdata_obj;

    return retval;
}
