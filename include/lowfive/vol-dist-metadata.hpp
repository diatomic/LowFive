#pragma     once

#include    <vector>
#include    <fmt/core.h>
#include    "vol-metadata.hpp"
#include    "index.hpp"
#include    "query.hpp"
#include    <diy/types.hpp>

namespace LowFive
{

// dataset intercommunicator (relevant for consumer only)
struct DataIntercomm
{
    std::string         filename;
    std::string         full_path;
    int                 intercomm_index;            // the intercomm to use for this dataset
};

// custom VOL object for distributed metadata
struct DistMetadataVOL: public LowFive::MetadataVOL
{
    using communicator      = diy::mpi::communicator;
    using communicators     = std::vector<communicator>;
    using ServeData         = Index::ServeData;
    using DataIntercomms    = std::vector<DataIntercomm>;

    communicator    local;
    communicators   intercomms;

    ServeData       serve_data;
    DataIntercomms  data_intercomms;

                    DistMetadataVOL(diy::mpi::communicator  local_,
                                    diy::mpi::communicator  intercomm_,
                                    bool                    memory_,
                                    bool                    passthru_,
                                    bool                    copy_ = true):
                        DistMetadataVOL(local_, communicators { std::move(intercomm_) }, memory_, passthru_, copy_)
                    {}

                    DistMetadataVOL(communicator            local_,
                                    communicators           intercomms_,
                                    bool                    memory_,
                                    bool                    passthru_,
                                    bool                    copy_ = true):
                        local(local_), intercomms(std::move(intercomms_))
                    {
                        vol_properties.memory   = memory_;
                        vol_properties.passthru = passthru_;
                        vol_properties.copy     = copy_;
                    }

    // record intercomm to use for a dataset
    void data_intercomm(std::string filename, std::string full_path, int intercomm_index)
    {
        data_intercomms.emplace_back(DataIntercomm { filename, full_path, intercomm_index });
    }

    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;
};

}
