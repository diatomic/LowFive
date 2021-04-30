#pragma     once

#include    <fmt/core.h>
#include    "vol-metadata.hpp"
#include    "index-query.hpp"
#include    <diy/types.hpp>

namespace LowFive
{

// custom VOL object for distributed metadata
struct DistMetadataVOL: public LowFive::MetadataVOL
{
    using communicator = diy::mpi::communicator;

    communicator    local;
    communicator    intercomm;
    bool            shared;

                    DistMetadataVOL(diy::mpi::communicator  local_,
                                    diy::mpi::communicator  intercomm_,
                                    bool                    shared_,
                                    bool                    memory_,
                                    bool                    passthru_,
                                    bool                    copy_ = true):
                        local(local_), intercomm(intercomm_), shared(shared_)
                    {
                        vol_properties.memory   = memory_;
                        vol_properties.passthru = passthru_;
                        vol_properties.copy     = copy_;
                    }

    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;
};

}
