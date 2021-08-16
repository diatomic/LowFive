#include <diy/mpi/communicator.hpp>
#include "prod-con-multidata.hpp"

using communicator = diy::mpi::communicator;

extern "C" {
void producer2_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared, std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points);
}

void producer2_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared, std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points)
{
    if (intercomms.size() == 2)
        fmt::print("producer: shared {} local size {}, intercomm1 size {} intercomm2 size {}\n",
                shared, local.size(), intercomms[0].size(), intercomms[1].size());
    else
        fmt::print("producer: shared {} local size {}, intercomm1 size {}\n",
                shared, local.size(), intercomms[0].size());

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomms, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // set ownership of dataset (default is user (shallow copy), lowfive means deep copy)
    // filename and full path to dataset can contain '*' and '?' wild cards (ie, globs, not regexes)
    vol_plugin.data_ownership("outfile.h5", "/group1/grid", l5::Dataset::Ownership::lowfive);
    vol_plugin.data_ownership("outfile.h5", "/group1/particles", l5::Dataset::Ownership::user);

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
    diy::ContiguousAssigner         prod_assigner(local.size(), global_nblocks);
    diy::RegularDecomposer<Bounds>  prod_decomposer(dim, domain, global_nblocks);
    prod_decomposer.decompose(local.rank(), prod_assigner, prod_create);

    // create a new file and group using default properties
    hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    domain_cnts[0]  = global_num_points;
    domain_cnts[1]  = DIM;

    // create the file data space for the particles
    hid_t filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

    // create the particle dataset with default properties
    hid_t dset = H5Dcreate2(group, "particles", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the particle data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_points(cp, dset, global_nblocks); });

    // signal the consumer that data are ready
    if (passthru && !metadata && !shared)
    {
        for (auto& intercomm: intercomms)
            intercomm.barrier();
    }

    else if (passthru && !metadata && shared)
    {
        local.barrier();
        int a = 0;                          // it doesn't matter what we send, for synchronization only
        for (const communicator& intercomm : intercomms)
            intercomm.send(local.rank(), 0, a);
    }

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);
}

