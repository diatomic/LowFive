#include    <utility>
#include    <vector>

#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "../prod-con-multidata/prod-con-multidata.hpp"

#include    <H5DSpublic.h>

using communicator = diy::mpi::communicator;

using Bounds = diy::DiscreteBounds;
using Point = diy::DynamicPoint<int>;
using Decomposer = diy::RegularDecomposer<Bounds>;
using Assigner = diy::ContiguousAssigner;

std::pair<hid_t, size_t> localize_dataspace(int rank, int n_ranks, const std::vector<hsize_t>& global_dims, hid_t dataspace_id)
{
    assert(global_dims.size() == DIM);

    Point domain_min {0, 0, 0};
    Point domain_max {0, 0, 0};
    for(size_t d = 0 ; d < DIM ; ++d)
        domain_max[d] = global_dims[d] - 1;

//    std::cerr << "localize_dataspace: rank = " << rank << ", n_ranks = " << n_ranks << std::endl;
//    std::cerr << "localize_dataspace: domain_max = " << domain_max << std::endl;

    Bounds domain {domain_min, domain_max};

    Decomposer::BoolVector wrap {true, true, true};
    Decomposer::BoolVector share_face {false, false, false};
    Decomposer::CoordinateVector ghosts {0, 0, 0};

    Decomposer decomposer {DIM, domain, n_ranks, share_face, wrap, ghosts};

//    print_bounds("localize_dataspace: domain", domain);
    // we use only core, no ghosts, so bounds is actually not needed
    Bounds core {DIM}, bounds {DIM};

    Assigner assigner {n_ranks, n_ranks};

    decomposer.decompose(rank, assigner, [&core, &bounds](int, const Bounds& _core, const Bounds& _bounds, const Bounds& _domain, Decomposer::Link) {
//      print_bounds("lambda: core", core);
//      print_bounds("lambda: _core", _core);
//      print_bounds("lambda: _bounds", _bounds);
//      print_bounds("lambda: _domain", _domain);
      core = _core;
      bounds = _bounds;
    });

//    std::cerr << "decompose done" << std::endl;

    std::vector<hsize_t> offsets {core.min.begin(), core.min.end()};
    std::vector<hsize_t> counts(DIM, 0);

    size_t core_size = 1;

    for(size_t d = 0 ; d < DIM ; ++d) {
        counts[d] = core.max[d] - core.min[d] + 1;
        core_size *= counts[d];
    }

    std::cerr << "consumer: core = [(" << core.min << "), (" << core.max << ")]" << ", core_size= " << core_size << std::endl;
//    for(auto c: counts) std::cerr << c << ", ";

    H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, offsets.data(), nullptr, counts.data(), nullptr);

    hid_t loc_dataspace_id = H5Screate_simple(DIM, counts.data(), counts.data());

    return {loc_dataspace_id, core_size};
}

herr_t fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
}

void nonneg(herr_t retval, char* msg)
{
    if (retval < 0) {
        fmt::print(stderr, "Error executing {}\n", msg);
        abort();
    }
}

void zero(herr_t retval, char* msg)
{
    if (retval) {
        fmt::print(stderr, "Error executing {}\n", msg);
        abort();
    }
}

int main(int argc, char** argv)
{
    int dim = DIM;

    diy::mpi::environment env(argc, argv, MPI_THREAD_MULTIPLE);
    diy::mpi::communicator world;

    int global_nblocks = world.size();   // global number of blocks
    int mem_blocks = -1;             // all blocks in memory
    int threads = 1;              // no multithreading
    std::string prefix = "./DIY.XXXXXX"; // for saving block files out of core
    // opts does not handle bool correctly, using int instead
    int metadata = 1;              // build in-memory metadata
    int passthru = 0;              // write file to disk
    bool shared = false;          // producer and consumer run on the same ranks
    size_t local_num_points = 20;             // points per block
    std::string log_level = "";
    std::string outfile = "outfile.h5";   // output file name

    (void)shared; // suppress warning

    // default global data bounds
    Bounds domain {dim};
    for(auto i = 0 ; i < dim ; i++) {
        domain.min[i] = 0;
        domain.max[i] = 4;
    }

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
            >> Option('n', "number", local_num_points, "number of points per block")
            >> Option('b', "blocks", global_nblocks, "number of blocks")
            >> Option('t', "thread", threads, "number of threads")
            >> Option("memblks", mem_blocks, "number of blocks to keep in memory")
            >> Option("prefix", prefix, "prefix for external storage")
            >> Option('m', "memory", metadata, "build and use in-memory metadata")
            >> Option('f', "file", passthru, "write file to disk")
            >> Option('l', "log", log_level, "level for the log output (trace, debug, info, ...)");
    ops
            >> Option('x', "max-x", domain.max[0], "domain max x")
            >> Option('y', "max-y", domain.max[1], "domain max y")
            >> Option('z', "max-z", domain.max[2], "domain max z");

    bool verbose, help;
    ops
            >> Option('v', "verbose", verbose, "print the block contents")
            >> Option('h', "help", help, "show help");

    if (!ops.parse(argc, argv) || help) {
        if (world.rank() == 0) {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]\n";
            std::cout << "Generates a grid and random particles in the domain and redistributes them into correct blocks.\n";
            std::cout << ops;
        }
        return 1;
    }

    if (!log_level.empty())
        l5::create_logger(log_level);

    // set HDF5 error handler
    H5Eset_auto(H5E_DEFAULT, fail_on_hdf5_error, NULL);

    // create the vol plugin
    l5::MetadataVOL& vol_plugin = l5::MetadataVOL::create_MetadataVOL();
    if (metadata)
        vol_plugin.set_memory(outfile, "*");
    if (passthru)
        vol_plugin.set_passthru(outfile, "*");

    vol_plugin.keep = true;

    communicator local;
    local.duplicate(world);

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);
    l5::H5VOLProperty vol_prop(vol_plugin);
    if (!getenv("HDF5_VOL_CONNECTOR")) {
        fmt::print("HDF5_VOL_CONNECTOR is not set; enabling VOL explicitly\n");
        vol_prop.apply(plist);
    } else {
        fmt::print("HDF5_VOL_CONNECTOR is set; not enabling VOL explicitly\n");
    }

    // diy setup
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
    diy::ContiguousAssigner prod_assigner(local.size(), global_nblocks);
    diy::RegularDecomposer<Bounds> prod_decomposer(dim, domain, global_nblocks);
    prod_decomposer.decompose(local.rank(), prod_assigner, prod_create);

    // create a new file and group using default properties
    hid_t file = H5Fcreate(outfile.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for(auto i = 0 ; i < dim ; i++)
        domain_cnts[i] = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the grid dataset with default properties
    hid_t dset = H5Dcreate(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the grid data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp) { b->write_block_grid(cp, dset); });

    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);

#if 0

    // open in consumer mode
    {
        hid_t plist_id, file_id, group_id, dataspace_id, dataset_id;
        herr_t status;

        file_id = H5Fopen(outfile.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

        if (file_id < 0) {
            std::cerr << "consumer ERROR opening file " << outfile << std::endl;
            exit(1);
        } else {
            std::cerr << "consumer file open OK " << outfile << std::endl;
        }

        dataset_id = H5Dopen2(file_id, "/group1/grid", H5P_DEFAULT);

        std::cerr << "consumer dataset_open OK " << outfile << std::endl;

        dataspace_id = H5Dget_space(dataset_id);

        std::cerr << "consumer dataspace open OK " << outfile << std::endl;

        std::vector<hsize_t> global_dims(DIM, 0);
        std::vector<hsize_t> maxdims(DIM, 0);

        H5Sget_simple_extent_dims(dataspace_id, global_dims.data(), maxdims.data());

        auto ldcs = localize_dataspace(local.rank(), local.size(), global_dims, dataspace_id);

        hid_t mem_dataspace_id = ldcs.first;
        size_t core_size = ldcs.second;

        std::vector<float> dset_data(core_size, 0);

        status = H5Dread(dataset_id, H5T_IEEE_F32LE, mem_dataspace_id, dataspace_id, H5P_DEFAULT, dset_data.data());
        if (status) {
            std::cerr << "error when reading" << std::endl;
            exit(1);
        }

        auto local_sum = std::accumulate(dset_data.begin(), dset_data.end(), 0.0);
        std::cerr << "local_sum = " << local_sum << std::endl;

        // close dataspace
        status = H5Sclose(dataspace_id);
        if (status) {
            std::cerr << "error when closing dataspace_id" << std::endl;
            exit(1);
        }

        status = H5Sclose(mem_dataspace_id);
        if (status) {
            std::cerr << "error when closing mem_dataspace_id" << std::endl;
            exit(1);
        }

        // close dataset
        status = H5Dclose(dataset_id);
        if (status) {
            std::cerr << "error when closing dataset_id" << std::endl;
            exit(1);
        }

        //  close file.
        status = H5Fclose(file_id);
        if (status) {
            std::cerr << "error when closing file_id" << std::endl;
            exit(1);
        }
    }
#endif

    return 0;
}

