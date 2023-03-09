#include <stddef.h>
#include <iostream>
#include <functional>

#include    <diy/../../examples/opts.h>

#include <diy/mpi/communicator.hpp>
#include "prod-con-multidata.hpp"

// to run: mpiexec -n 1 -l rr ./prod-metadata-only -l trace -m 1 -f 0
// to run w/o segfault: mpiexec -n 1 -l rr ./prod-metadata-only -l trace -m 1 -f 0 -k
// to run w/o L5: mpiexec -n 1 -l rr ./prod-metadata-only -n

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    MPI_Comm comm_;
    MPI_Comm_dup(MPI_COMM_WORLD, &comm_);

    diy::mpi::communicator comm(comm_);


    int metadata = 1;
    int passthru = 0;
    bool keep {false};
    bool help {false};
    bool close {false};
    bool no_vol {false};
    std::string log_level {"info"};

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
            >> Option('m', "memory",    metadata,       "build and use in-memory metadata")
            >> Option('f', "file",      passthru,       "write file to disk")
            >> Option('l', "log",       log_level,      "level for the log output (trace, debug, info, ...)")
            >> Option('k', "keep",      keep,           "keep LowFive metadata in memory")
            >> Option('c', "close",     close,          "close all HDF5 objects at exit (set to false to check for final SEGFAULT)")
            >> Option('h', "help",      help,           "show help")
            >> Option('n', "no-vol",    no_vol,         "do not create LowFive VOL, use plain HDF5")
            ;


    if (!ops.parse(argc,argv) || help)
    {
        if (comm.rank() == 0)
        {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]\n";
            std::cout << ops;
        }
        return 1;
    }

    if (!log_level.empty())
        l5::create_logger(log_level);

    const bool use_vol = !no_vol;

    bool pause_for_debug = false;

    if (pause_for_debug) {
        // for external debugging:
        // 1. each rank prints its pid
        // 2. each rank is stuck in the while loop
        // 3. until we attach to one of the ranks and change blocker to 1
        // 4. we broadcast the new value to all ranks and all ranks continue
        auto pid = getpid();
        std::cerr << "pid = " << pid << std::endl;

        int blocker = 0;
        while(blocker == 0) {
            sleep(1);
            MPI_Allreduce(MPI_IN_PLACE, static_cast<void*>(&blocker), 1, MPI_INT, MPI_SUM, comm_);
        }
    }

    const int dim = 3;

    const int threads = 1;
    const int mem_blocks = -1;
    const int global_nblocks = comm.size();
    const size_t local_num_points = 100;

    Bounds domain { dim };
    for (auto i = 0; i < dim; i++)
    {
        domain.min[i] = 0;
        domain.max[i] = 10;
    }

    l5::MetadataVOL& vol = l5::MetadataVOL::create_MetadataVOL();

    //vol.set_keep(keep);

    // set up lowfive
    l5::MetadataVOL* vol_plugin = use_vol ? &vol : nullptr;
    if (use_vol)
        vol_plugin->set_keep(keep);


    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, comm_, MPI_INFO_NULL);

    if (use_vol) {
        l5::H5VOLProperty vol_prop(*vol_plugin);
        if (!getenv("HDF5_VOL_CONNECTOR")) {
            fmt::print(stderr, "HDF5_VOL_CONNECTOR is not set; enabling VOL explicitly\n");
            vol_prop.apply(plist);
        } else {
            fmt::print(stderr, "HDF5_VOL_CONNECTOR is set; not enabling VOL explicitly\n");
        }

        // set ownership of dataset (default is user (shallow copy), lowfive means deep copy)
        // filename and full path to dataset can contain '*' and '?' wild cards (ie, globs, not regexes)
        // NB: we say nothing about /group1/grid because lowfive ownership is implicit
        if (passthru)
            vol_plugin->set_passthru("outfile.h5", "*");
        if (metadata)
            vol_plugin->set_memory("outfile.h5", "*");
        vol_plugin->set_zerocopy("outfile.h5", "/group1/particles");
    }

    std::string prefix = "";

    // diy setup for the producer
    diy::FileStorage                prod_storage(prefix);
    diy::Master                     prod_master(comm,
            threads,
            mem_blocks,
            &Block::create,
            &Block::destroy,
            &prod_storage,
            &Block::save,
            &Block::load);
    size_t global_num_points = local_num_points * global_nblocks;
    AddBlock                        prod_create(prod_master, local_num_points, global_num_points, global_nblocks);
    diy::ContiguousAssigner         prod_assigner(comm.size(), global_nblocks);
    diy::RegularDecomposer<Bounds>  prod_decomposer(dim, domain, global_nblocks);
    prod_decomposer.decompose(comm.rank(), prod_assigner, prod_create);

    // create a new file and group using default properties
    hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < static_cast<decltype(i)>(DIM); i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

//    {
//        // create the file data space for the global grid
//        hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);
//
//        // create the grid dataset with default properties
//        hid_t dset = H5Dcreate2(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
//
//        // write the grid data
//        prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp) { b->write_block_grid(cp, dset); });
//
//        std::cerr << "(before first close) dataspace = " << filespace << ", H5Iget_ref = " << H5Iget_ref(filespace) << std::endl;
//        std::cerr << "(before first close) dataset   = " << dset << ", H5Iget_ref = " << H5Iget_ref(dset) << std::endl;
//
//        if (close) {
//            // clean up
//            H5Dclose(dset);
//            H5Sclose(filespace);
//            std::cerr << "Closed first dset (grids) and its dataspace" << std::endl;
//        }
//    }

    {

        // create the file data space for the particles
        domain_cnts[0] = global_num_points;
        domain_cnts[1] = DIM;
        hid_t filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

        // create the particle dataset with default properties
        hid_t dset = H5Dcreate2(group, "particles", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // write the particle data
        prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp) { b->write_block_points(cp, dset, global_nblocks); });

//        std::cerr << "file      = " << file << ", H5Iget_ref = " << H5Iget_ref(file) << std::endl;
//        std::cerr << "dataspace = " << filespace << ", H5Iget_ref = " << H5Iget_ref(filespace) << std::endl;
//        std::cerr << "dataset   = " << dset << ", H5Iget_ref = " << H5Iget_ref(dset) << std::endl;
//        std::cerr << "group     = " << group << ", H5Iget_ref = " << H5Iget_ref(group) << std::endl;

        if (close) {
            // clean up
            H5Dclose(dset);
            H5Sclose(filespace);
            H5Gclose(group);

            //H5close();

            H5Fclose(file);
            H5Pclose(plist);
        }
    }
}

