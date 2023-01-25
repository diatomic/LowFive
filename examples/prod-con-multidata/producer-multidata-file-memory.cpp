#include <stddef.h>

#include <diy/mpi/communicator.hpp>
#include "prod-con-multidata.hpp"

using communicator = MPI_Comm;
using diy_comm = diy::mpi::communicator;

extern "C" {
void producer_f(communicator& local, const std::vector<communicator>& intercomms,
        std::mutex& exclusive, bool shared, std::string prefix,
        int metadata, int passthru,
        int threads, int mem_blocks,
        Bounds domain,
        int global_nblocks, int dim, size_t local_num_points);
}

void producer_f(communicator& local, const std::vector<communicator>& intercomms,
        std::mutex& exclusive, bool shared, std::string prefix,
        int metadata, int passthru,
        int threads, int mem_blocks,
        Bounds domain,
        int global_nblocks, int dim, size_t local_num_points)
{
    diy::mpi::communicator local_(local);

    if (!passthru || !metadata)
        throw std::runtime_error("This test is for passthru + memory only");

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomms);

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    l5::H5VOLProperty vol_prop(vol_plugin);
    if (!getenv("HDF5_VOL_CONNECTOR")) {
        fmt::print(stderr, "HDF5_VOL_CONNECTOR is not set; enabling VOL explicitly\n");
        vol_prop.apply(plist);
    } else {
        fmt::print(stderr, "HDF5_VOL_CONNECTOR is set; not enabling VOL explicitly\n");
    }

    // set ownership of dataset (default is user (shallow copy), lowfive means deep copy)
    // filename and full path to dataset can contain '*' and '?' wild cards (ie, globs, not regexes)
    // NB: we say nothing about /group1/grid because lowfive ownership is implicit
    vol_plugin.set_passthru("outfile.h5", "/group1/*");
    vol_plugin.set_memory("outfile.h5", "/group2/*");
    vol_plugin.set_zerocopy("outfile.h5", "/group1/particles");

    // diy setup for the producer
    diy::FileStorage prod_storage(prefix);
    diy::Master prod_master(local,
            threads,
            mem_blocks,
            &Block::create,
            &Block::destroy,
            &prod_storage,
            &Block::save,
            &Block::load);
    size_t global_num_points = local_num_points * global_nblocks;
    AddBlock prod_create(prod_master, local_num_points, global_num_points, global_nblocks);
    diy::ContiguousAssigner prod_assigner(local_.size(), global_nblocks);
    diy::RegularDecomposer<Bounds> prod_decomposer(dim, domain, global_nblocks);
    prod_decomposer.decompose(local_.rank(), prod_assigner, prod_create);

    // create a new file and group_1 using default properties
    hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group_1 = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    hid_t group_2 = H5Gcreate(file, "/group2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for(auto i = 0 ; i < static_cast<decltype(i)>(DIM) ; i++)
        domain_cnts[i] = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    // data space shared by both datasets
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the grid dataset with default properties
    hid_t dset_1 = H5Dcreate2(group_1, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the grid data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp) { b->write_block_grid(cp, dset_1); });

    // clean up
    H5Dclose(dset_1);
    H5Gclose(group_1);
    H5Sclose(filespace);

    // create the file data space for the particles
    domain_cnts[0] = global_num_points;
    domain_cnts[1] = DIM;
    filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

    // create the particle dataset with default properties
    hid_t dset_2 = H5Dcreate2(group_2, "particles", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the particle data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp) { b->write_block_points(cp, dset_2, global_nblocks); });

    // clean up
    H5Dclose(dset_2);
    H5Sclose(filespace);
    H5Gclose(group_2);

    H5Fclose(file);
    H5Pclose(plist);

    // for passthru and metatada, order of operations in file_open/file_close ensures synchronization
}

