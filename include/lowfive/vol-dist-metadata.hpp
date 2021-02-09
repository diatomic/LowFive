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
    diy::mpi::
    communicator    world;

    Index*          index;

                    DistMetadataVOL(diy::mpi::communicator& world_):
                        world(world_), index(nullptr)                               {}
                    ~DistMetadataVOL()
                    {
                        if (index)
                            delete index;
                    }

    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
};

}
