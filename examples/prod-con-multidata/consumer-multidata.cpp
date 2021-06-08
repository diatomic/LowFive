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

    diy::mpi::communicator intercomm;
    if (shared)
    {
        fmt::print("Consumer local comm = {}\n", static_cast<intptr_t>(local.handle()));
        intercomm = world;
    } else
    {
        // split the world into producer and consumer
        local = world.split(producer);

        MPI_Comm intercomm_;
        MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer ? producer_ranks : 0, /* tag = */ 0, &intercomm_);
        intercomm = diy::mpi::communicator(intercomm_);
    }
    fmt::print("local.size() = {}, intercomm.size() = {}\n", local.size(), intercomm.size());

    // Set up file access property list with mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    //H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomm, shared, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // wait for data to be ready
    if (passthru && !metadata)
        intercomm.barrier();

    // create a new file using default properties
    hid_t file = H5Fopen("outfile.h5", H5F_ACC_RDONLY, plist);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < DIM; i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // open the grid dataset
    hid_t dset = H5Dopen(file, "/group1/grid", H5P_DEFAULT);

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
    size_t local_num_points = global_num_points / con_nblocks;  // TODO: assumes global_num_points divides con_nblocks w/o remainder
    AddBlock                        con_create(con_master, local_num_points, global_num_points, con_nblocks);
    diy::ContiguousAssigner         con_assigner(local.size(), con_nblocks);
    diy::RegularDecomposer<Bounds>  con_decomposer(dim, domain, con_nblocks);
    con_decomposer.decompose(local.rank(), con_assigner, con_create);

    // read the grid data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_grid(cp, dset); });

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);

    // create the file data space for the particles
    domain_cnts[0]  = global_num_points;             // same number of particles in each block
    domain_cnts[1]  = DIM;
    filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

    fmt::print(stderr, "consumer: local_num_points {} global_num_points {} con_nblocks {}\n",
            local_num_points, global_num_points, con_nblocks);

    // open the particle dataset
    dset = H5Dopen(file, "/group1/particles", H5P_DEFAULT);

    // read the particle data
    con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_points(cp, dset, global_num_points, con_nblocks); });

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Fclose(file);
    H5Pclose(plist);
}       // consumer ranks

