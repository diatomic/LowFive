#pragma     once

#include    <vector>
#include    <set>
#include    <mpi.h>

#include    "vol-metadata.hpp"

namespace LowFive
{

struct Dataset;
using Datasets = std::set<Dataset*>;

// custom VOL object for distributed metadata
struct DistMetadataVOL: public LowFive::MetadataVOL
{
    using communicator      = MPI_Comm;
    using communicators     = std::vector<communicator>;
    using DataIntercomms    = std::vector<int>;
    using FileNames         = std::vector<std::string>;

    communicator    local;
    communicators   intercomms;

    // parallel vectors to make use of the API in MetadataVOL
    LocationPatterns    intercomm_locations;
    DataIntercomms      intercomm_indices;

    bool                serve_on_close {true};

                    DistMetadataVOL(communicator  local_, communicator  intercomm_);

                    DistMetadataVOL(communicator            local_,
                                    communicators           intercomms_):
                        local(local_), intercomms(std::move(intercomms_))
                    {}

    // record intercomm to use for a dataset
    void set_intercomm(std::string filename, std::string full_path, int intercomm_index)
    {
        intercomm_locations.emplace_back(LocationPattern { filename, full_path });
        intercomm_indices.emplace_back(intercomm_index);
    }

    void            serve_all(bool delete_data = true);

    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;

    void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;

    void*           group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req) override;

    int             remote_size(int intercomm_index);

    FileNames       get_filenames(int intercomm_index);
    void            send_done(int intercomm_index);
    void            producer_signal_done();

    long int        file_close_counter_ {0};  // increment after each call to file_close

};

}
