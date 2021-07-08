#include <diy/mpi/communicator.hpp>
#include "prod-con-multidata.hpp"

using communicator = diy::mpi::communicator;

extern "C" {
void producer_f (communicator& world, communicator local, std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks, int consumer1_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points);
}

void producer_f (communicator& world, communicator local, std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks, int consumer1_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int global_nblocks, int dim, size_t local_num_points)
{
    fmt::print("Entered producer\n");

    int task = producer_task;                       // for splitting communicator


    using communicator = diy::mpi::communicator;
    MPI_Comm intercomm1_, intercomm2_;
    std::vector<communicator> communicators;

    if (shared)
        communicators.push_back(world);
    else
    {
        // split the world into producer and consumer
        local = world.split(task);

        MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer_ranks, /* tag = */ 0, &intercomm1_);
        MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer_ranks + consumer1_ranks, /* tag = */ 0, &intercomm2_);
        communicators.push_back(communicator(intercomm1_));
        communicators.push_back(communicator(intercomm2_));
    }
    fmt::print("producer: shared {} local size {}, intercomm1 size {} intercomm2 size {}\n",
            shared, local.size(), communicators[0].size(), communicators[1].size());

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, communicators, shared, metadata, passthru);
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
    for (auto i = 0; i < DIM; i++)
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

    // create the file data space for the particles
    domain_cnts[0]  = global_num_points;
    domain_cnts[1]  = DIM;
    filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

    // create the particle dataset with default properties
    dset = H5Dcreate2(group, "particles", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the particle data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_points(cp, dset, global_nblocks); });

    // signal the consumer that data are ready
    if (passthru && !metadata && !shared)
    {
        for (auto& intercomm: communicators)
        intercomm.barrier();
    }

    else if (passthru && !metadata && shared)
    {
        world.barrier();
        int a = 0;                          // it doesn't matter what we send, for synchronization only
        world.send(local.rank(), 0, a);
    }

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);

    if (!shared)
    {
        MPI_Comm_free(&intercomm1_);
        MPI_Comm_free(&intercomm2_);
    }
}

