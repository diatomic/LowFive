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

    using ServeIndices           = std::function<DataIntercomms()>;
    using SetFileName            = std::function<std::string()>;
    using SendFileName           = std::function<void(const std::string&)>;
    using NoncollectiveDatasets  = std::function<std::set<std::string>()>;

    communicator    local;
    communicators   intercomms;

    // parallel vectors to make use of the API in MetadataVOL
    LocationPatterns    intercomm_locations;
    DataIntercomms      intercomm_indices;

    bool                serve_on_close {true};

    // callbacks
    ServeIndices                 serve_indices;
    SetFileName                  set_filename;
    SendFileName                 send_filename;
    NoncollectiveDatasets        noncollective_dataset_writes;

    protected:
                    DistMetadataVOL(communicator  local_, communicator  intercomm_);

                    DistMetadataVOL(communicator            local_,
                                    communicators           intercomms_):
                        local(local_), intercomms(std::move(intercomms_))
                    {}
    public:

                    // prohibit copy and move
                    DistMetadataVOL(const DistMetadataVOL&)=delete;
                    DistMetadataVOL(DistMetadataVOL&&)=delete;

                    DistMetadataVOL& operator=(const DistMetadataVOL&)=delete;
                    DistMetadataVOL& operator=(DistMetadataVOL&&)=delete;

    static DistMetadataVOL&         create_DistMetadataVOL(communicator local_, communicator intercomm_);
    static DistMetadataVOL&         create_DistMetadataVOL(communicator local_, communicators intercomms_);

    // record intercomm to use for a dataset
    void set_intercomm(std::string filename, std::string full_path, int intercomm_index)
    {
        intercomm_locations.emplace_back(LocationPattern { filename, full_path });
        intercomm_indices.emplace_back(intercomm_index);
    }

    void set_serve_indices(ServeIndices si)
    {
        serve_indices = si;
    }

    void set_consumer_filename(SetFileName name)
    {
        set_filename = name;
    }

    void set_send_filename(SendFileName sfn)
    {
        send_filename = sfn;
    }

    void set_noncollective_datasets(NoncollectiveDatasets nd)
    {
        noncollective_dataset_writes = nd;
    }

    void unset_dist_callbacks()
    {
        serve_indices = nullptr;
        set_filename = nullptr;
        send_filename = nullptr;
        noncollective_dataset_writes = nullptr;
    }

    void            broadcast_files(int root = 0);

    void            serve_all(bool delete_data = true, bool perform_indexing = true);

    void*           object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req) override;
    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;

    void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;

    int             remote_size(int intercomm_index);

    FileNames       get_filenames(int intercomm_index);
    void            send_done(int intercomm_index);
    void            producer_signal_done();

    long int        file_close_counter_ {0};  // increment after each call to file_close

    private:
        void        make_remote_dataset(ObjectPointers*& result, std::pair<std::string, std::string> filepath);
};

}
