#include <diy/mpi/communicator.hpp>
#include <thread>
#include "prod-con-multidata.hpp"

using communicator = diy::mpi::communicator;

extern "C"
{
void consumer_f (communicator& world, communicator local, std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int con_nblocks, int dim, size_t global_num_points);
}

// --- ranks of consumer task ---
void consumer_f (communicator& world, communicator local, std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int con_nblocks, int dim, size_t global_num_points)
{
    fmt::print("Entered consumer\n");

    bool producer = false;

    MPI_Comm intercomm_;
    diy::mpi::communicator intercomm;

    if (shared)
        intercomm   = world;
    else
    {
        // split the world into producer and consumer
        local = world.split(producer);

        MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer ? producer_ranks : 0, /* tag = */ 0, &intercomm_);
        intercomm = diy::mpi::communicator(intercomm_);
    }
    fmt::print("local.size() = {}, intercomm.size() = {}\n", local.size(), intercomm.size());

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomm, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // wait for data to be ready
    if (passthru && !metadata && !shared)
        intercomm.barrier();

    else if (passthru && !metadata && shared)
    {
        int a;                                  // it doesn't matter what we receive, for synchronization only
        world.recv(local.rank(), 0, a);
    }

    // --- consumer ranks running user task code ---

    // diy setup for the consumer task on the consumer side
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
    diy::ContiguousAssigner         con_assigner(local.size(), con_nblocks);
    diy::RegularDecomposer<Bounds>  con_decomposer(dim, domain, con_nblocks);
    con_decomposer.decompose(local.rank(), con_assigner, con_create);

    // open the file and the dataset
    hid_t file = H5Fopen("outfile.h5", H5F_ACC_RDONLY, plist);
    hid_t dset = H5Dopen(file, "/group1/grid", H5P_DEFAULT);

    // read the grid data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_grid(cp, dset); });

    // clean up
    H5Dclose(dset);

    // open the particle dataset
    dset = H5Dopen(file, "/group1/particles", H5P_DEFAULT);

    // read the particle data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_points(cp, dset, global_num_points, con_nblocks); });

    // clean up
    H5Dclose(dset);
    H5Fclose(file);
    H5Pclose(plist);

    if (!shared)
        MPI_Comm_free(&intercomm_);
}

