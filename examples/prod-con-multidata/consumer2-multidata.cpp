#include <stddef.h>

#include <diy/mpi/communicator.hpp>
#include <thread>
#include "prod-con-multidata.hpp"

using communicator = MPI_Comm;
using diy_comm = diy::mpi::communicator;

extern "C"
{
void consumer2_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 int con_nblocks);
}

void consumer2_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 int con_nblocks)
{
    diy::mpi::communicator local_(local);

    //if (intercomms.size() == 2)
    //    fmt::print(stderr, "consumer2: shared {} local size {} intercomms size {} intercomm1 size {} intercomm2 size {}\n",
    //            shared, local.size(), intercomms.size(), intercomms[0].size(), intercomms[1].size());
    //else
    //    fmt::print(stderr, "consumer2: shared {} local size {} intercomms size {} intercomm1 size {}\n",
    //            shared, local.size(), intercomms.size(), intercomms[0].size());

    // set up lowfive
    l5::DistMetadataVOL& vol_plugin = LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomms);

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    l5::H5VOLProperty vol_prop(vol_plugin);
    if (!getenv("HDF5_VOL_CONNECTOR"))
    {
        fmt::print(stderr, "HDF5_VOL_CONNECTOR is not set; enabling VOL explicitly\n");
        vol_prop.apply(plist);
    } else
    {
        fmt::print(stderr, "HDF5_VOL_CONNECTOR is set; not enabling VOL explicitly\n");
    }

    if (passthru)
        vol_plugin.set_passthru("outfile.h5", "*");
    if (metadata)
        vol_plugin.set_memory("outfile.h5", "*");

    // set intercomms of dataset
    // filename and full path to dataset can contain '*' and '?' wild cards (ie, globs, not regexes)
    vol_plugin.set_intercomm("outfile.h5", "/group1/particles", 0);

    // wait for data to be ready
    if (passthru && !metadata && !shared)
    {
        for (auto& intercomm: intercomms)
            diy_comm(intercomm).barrier();
    }

    else if (passthru && !metadata && shared)
    {
        int a;                                  // it doesn't matter what we receive, for synchronization only
        for (auto& intercomm: intercomms)
            diy_comm(intercomm).recv(local_.rank(), 0, a);
    }

    // open the file, dataset, dataspace
    hid_t file              = H5Fopen("outfile.h5", H5F_ACC_RDONLY, plist);
    hid_t dset_particles    = H5Dopen(file, "/group1/particles", H5P_DEFAULT);
    hid_t dspace_particles  = H5Dget_space(dset_particles);

    // get global domain bounds
    int dspace_dim = H5Sget_simple_extent_ndims(dspace_particles);  // 2d [particle id][coordinate id]
    std::vector<hsize_t> min_(dspace_dim), max_(dspace_dim);
    H5Sget_select_bounds(dspace_particles, min_.data(), max_.data());
    fmt::print(stderr, "Dataspace extent: [{}] [{}]\n", fmt::join(min_, ","), fmt::join(max_, ","));
    int dim = max_[dspace_dim - 1] + 1;                             // 3d (extent of coordinate ids)
    Bounds domain { dim };
    {
        // because these are particles in 3d, any 3d decomposition will do
        // as long as it can be decomposed discretely into con_blocks
        // using con_blocks^dim, larger than necessary
        // no attempt to have points be inside of the block bounds (maybe not realistic)
        for (int i = 0; i < dim; ++i)
        {
            domain.min[i] = 0;
            domain.max[i] = con_nblocks;
        }
    }
    fmt::print(stderr, "Read domain: {} {}\n", domain.min, domain.max);

    // get global number of particles
    size_t global_num_points;
    {
        std::vector<hsize_t> min_(1), max_(1);
        H5Sget_select_bounds(dspace_particles, min_.data(), max_.data());
        global_num_points = max_[0] + 1;
    }
    fmt::print(stderr, "Global num points: {}\n", global_num_points);

    // diy setup for the consumer
    diy::FileStorage                con_storage(prefix);
    diy::Master                     con_master(local,
            threads,
            mem_blocks,
            &Block::create,
            &Block::destroy,
            &con_storage,
            &Block::save,
            &Block::load);
    size_t local_num_points = global_num_points / con_nblocks;
    AddBlock                        con_create(con_master, local_num_points, global_num_points, con_nblocks);
    diy::ContiguousAssigner         con_assigner(local_.size(), con_nblocks);
    diy::RegularDecomposer<Bounds>  con_decomposer(dim, domain, con_nblocks);
    con_decomposer.decompose(local_.rank(), con_assigner, con_create);

    // read the particle data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_points(cp, dset_particles, global_num_points, con_nblocks); });

    // clean up
    H5Sclose(dspace_particles);
    H5Dclose(dset_particles);
    H5Fclose(file);
    H5Pclose(plist);
}

