#include <diy/mpi/communicator.hpp>
#include <thread>
#include "prod-con-multidata.hpp"

using communicator = diy::mpi::communicator;

extern "C"
{
void consumer1_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int con_nblocks, int dim, size_t global_num_points);
}

void consumer1_f (communicator& local, const std::vector<communicator>& intercomms,
                 std::mutex& exclusive, bool shared,
                 std::string prefix, int producer_ranks,
                 int metadata, int passthru,
                 int threads, int mem_blocks,
                 Bounds domain,
                 int con_nblocks, int dim, size_t global_num_points)
{
    fmt::print("consumer1: shared {} local size {}, intercomm size {}\n", shared, local.size(), intercomms[0].size());

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomms[0], metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // wait for data to be ready
    if (passthru && !metadata && !shared)
        intercomms[0].barrier();

    else if (passthru && !metadata && shared)
    {
        int a;                                  // it doesn't matter what we receive, for synchronization only
        intercomms[0].recv(local.rank(), 0, a);
    }

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
    H5Fclose(file);
    H5Pclose(plist);
}

