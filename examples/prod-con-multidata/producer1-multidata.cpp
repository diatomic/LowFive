#include <stddef.h>

#include <diy/mpi/communicator.hpp>
#include "prod-con-multidata.hpp"

using communicator = MPI_Comm;
using diy_comm = diy::mpi::communicator;

extern "C" {
void producer1_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared, std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points);
}

void producer1_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared, std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points)
{
    diy::mpi::communicator local_(local);

    //if (intercomms.size() == 2)
    //    fmt::print(stderr, "producer1: shared {} local size {} intercomms size {} intercomm1 size {} intercomm2 size {}\n",
    //            shared, local.size(), intercomms.size(), intercomms[0].size(), intercomms[1].size());
    //else
    //    fmt::print(stderr, "producer1: shared {} local size {} intercomms size {} intercomm1 size {}\n",
    //            shared, local.size(), intercomms.size(), intercomms[0].size());

    // set up lowfive
    l5::DistMetadataVOL& vol_plugin = l5::DistMetadataVOL::create_DistMetadataVOL(local, intercomms);

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
        vol_plugin.set_passthru("outfile1.h5", "*");
    if (metadata)
        vol_plugin.set_memory("outfile1.h5", "*");
    vol_plugin.set_zerocopy("outfile1.h5", "/group1/grid");

    // diy setup for the producer
    diy::FileStorage                prod_storage(prefix);
    diy::Master                     prod_master(local,
            threads,
            mem_blocks,
            &Block::create,
            &Block::destroy,
            &prod_storage,
            &Block::save,
            &Block::load);
    size_t global_num_points = local_num_points * global_nblocks;
    AddBlock                        prod_create(prod_master, local_num_points, global_num_points, global_nblocks);
    diy::ContiguousAssigner         prod_assigner(local_.size(), global_nblocks);
    diy::RegularDecomposer<Bounds>  prod_decomposer(dim, domain, global_nblocks);
    prod_decomposer.decompose(local_.rank(), prod_assigner, prod_create);

    // create a new file and group using default properties
    hid_t file = H5Fcreate("outfile1.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < static_cast<decltype(i)>(DIM); i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the grid dataset with default properties
    hid_t dset = H5Dcreate2(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the grid data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_grid(cp, dset); });

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);

    // signal the consumer that data are ready
    if (passthru && !metadata && !shared)
    {
        for (auto& intercomm: intercomms)
            diy_comm(intercomm).barrier();
    }

    else if (passthru && !metadata && shared)
    {
        local_.barrier();
        int a = 0;                          // it doesn't matter what we send, for synchronization only
        for (auto& intercomm : intercomms)
            diy_comm(intercomm).send(local_.rank(), 0, a);
    }
}

