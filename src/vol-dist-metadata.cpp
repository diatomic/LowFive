#include <lowfive/vol-dist-metadata.hpp>

herr_t
LowFive::DistMetadataVOL::
dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

//     fmt::print("dset = {}\nmem_space_id = {} mem_space = {}\nfile_space_id = {} file_space = {}\n",
//                fmt::ptr(dset_->h5_obj),
//                mem_space_id, Dataspace(mem_space_id),
//                file_space_id, Dataspace(file_space_id));

    Dataset* ds = (Dataset*) dset_->mdata_obj;              // dataset from our metadata

    index->query(ds->data, Dataspace(file_space_id), Dataspace(mem_space_id), buf);

    return 0;
//     return VOLBase::dataset_read(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}

herr_t
LowFive::DistMetadataVOL::
dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    ObjectPointers* dset_ = (ObjectPointers*) dset;

//     fmt::print("dset = {}\nmem_space_id = {} ({})\nfile_space_id = {} ({})\n",
//                fmt::ptr(dset_->h5_obj),
//                mem_space_id, Dataspace(mem_space_id),
//                file_space_id, Dataspace(file_space_id));

    // save our metadata
    Dataset* ds = (Dataset*) dset_->mdata_obj;
    ds->write(Datatype(mem_type_id), Dataspace(mem_space_id), Dataspace(file_space_id), buf);

    index = new Index(world, ds);

//     fmt::print("Index created\n");
//     index->print();

    return 0;
//     return VOLBase::dataset_write(dset_->h5_obj, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
}
