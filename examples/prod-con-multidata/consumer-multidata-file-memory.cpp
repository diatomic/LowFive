#include <stddef.h>

#include <diy/mpi/communicator.hpp>
#include <thread>
#include "prod-con-multidata.hpp"

using communicator = MPI_Comm;
using diy_comm = diy::mpi::communicator;

extern "C"
{
void consumer_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 int con_nblocks);
}

void consumer_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 int con_nblocks)
{
    diy::mpi::communicator local_(local);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomms);

    if (!passthru || !metadata)
        throw std::runtime_error("This test is for passthru + memory only");

    if (intercomms.size() != 1)
        throw std::runtime_error("This test is for 1 producer, 1 file");

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
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

    vol_plugin.set_passthru("outfile.h5", "/group1/*");
    vol_plugin.set_memory("outfile.h5", "/group2/*");

    // set intercomms for memory dataset
    vol_plugin.set_intercomm("outfile.h5", "/group2/particles", 0);

    // open the file, datasets, dataspaces
    hid_t file, dset_grid, dspace_grid, dset_particles, dspace_particles;

    file              = H5Fopen("outfile.h5", H5F_ACC_RDONLY, plist);
    dset_grid         = H5Dopen(file, "/group1/grid", H5P_DEFAULT);
    dspace_grid       = H5Dget_space(dset_grid);
    dset_particles    = H5Dopen(file, "/group2/particles", H5P_DEFAULT);
    dspace_particles  = H5Dget_space(dset_particles);

    // get global domain bounds
    int dim = H5Sget_simple_extent_ndims(dspace_grid);
    Bounds domain { dim };
    {
        std::vector<hsize_t> min_(dim), max_(dim);
        H5Sget_select_bounds(dspace_grid, min_.data(), max_.data());
        for (int i = 0; i < dim; ++i)
        {
            domain.min[i] = min_[i];
            domain.max[i] = max_[i];
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

    // read the grid data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_grid(cp, dset_grid); });

    // read the particle data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_points(cp, dset_particles, global_num_points, con_nblocks); });

    // clean up
    H5Sclose(dspace_grid);
    H5Sclose(dspace_particles);
    H5Dclose(dset_grid);
    H5Dclose(dset_particles);
    H5Fclose(file);
    H5Pclose(plist);
}

