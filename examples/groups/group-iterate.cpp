#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "../prod-con-multidata/prod-con-multidata.hpp"

#include    <H5DSpublic.h>

using communicator = diy::mpi::communicator;

herr_t fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
}

// callback function for iterating over group, prints the member name
herr_t get_name(hid_t group_id, const char *member_name, void *target_name)
{
    fmt::print(stderr, "visiting {}\n", member_name);
    return 0;
}

// callback function for iterating over group, checks for target name
herr_t check_name(hid_t group_id, const char *member_name, void *target_name)
{
    fmt::print(stderr, "visiting {}\n", member_name);
    if (!strcmp(member_name, (char*)target_name))
    {
        fmt::print(stderr, "found {}\n", member_name);
        return 1;
    }
    return 0;
}

int main(int argc, char**argv)
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv, MPI_THREAD_MULTIPLE);
    diy::mpi::communicator    world;

    int                       global_nblocks    = world.size();   // global number of blocks
    int                       mem_blocks        = -1;             // all blocks in memory
    int                       threads           = 1;              // no multithreading
    std::string               prefix            = "./DIY.XXXXXX"; // for saving block files out of core
    // opts does not handle bool correctly, using int instead
    int                       metadata          = 1;              // build in-memory metadata
    int                       passthru          = 0;              // write file to disk
    bool                      shared            = false;          // producer and consumer run on the same ranks
    size_t                    local_num_points  = 20;             // points per block
    std::string               log_level         = "";
    std::string               outfile           = "outfile.h5";   // output file name

    (void) shared; // suppress warning

    // default global data bounds
    Bounds domain { dim };
    for (auto i = 0; i < dim; i++)
    {
        domain.min[i] = 0;
        domain.max[i] = 4;
    }

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
        >> Option('n', "number",    local_num_points,"number of points per block")
        >> Option('b', "blocks",    global_nblocks, "number of blocks")
        >> Option('t', "thread",    threads,        "number of threads")
        >> Option(     "memblks",   mem_blocks,     "number of blocks to keep in memory")
        >> Option(     "prefix",    prefix,         "prefix for external storage")
        >> Option('m', "memory",    metadata,       "build and use in-memory metadata")
        >> Option('f', "file",      passthru,       "write file to disk")
        >> Option('l', "log",       log_level,      "level for the log output (trace, debug, info, ...)")
        ;
    ops
        >> Option('x',  "max-x",    domain.max[0],  "domain max x")
        >> Option('y',  "max-y",    domain.max[1],  "domain max y")
        >> Option('z',  "max-z",    domain.max[2],  "domain max z")
        ;

    bool verbose, help;
    ops
        >> Option('v', "verbose",   verbose,        "print the block contents")
        >> Option('h', "help",      help,           "show help")
        ;

    if (!ops.parse(argc,argv) || help)
    {
        if (world.rank() == 0)
        {
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
    communicator local;
    local.duplicate(world);

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (passthru)
        H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);
    l5::H5VOLProperty vol_prop(vol_plugin);
    if (!getenv("HDF5_VOL_CONNECTOR"))
    {
        fmt::print("HDF5_VOL_CONNECTOR is not set; enabling VOL explicitly\n");
        vol_prop.apply(plist);
    } else
    {
        fmt::print("HDF5_VOL_CONNECTOR is set; not enabling VOL explicitly\n");
    }

    // diy setup
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
    hid_t file = H5Fcreate(outfile.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < dim; i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the grid dataset with default properties
    hid_t dset = H5Dcreate(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

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

    // iterate over the objects in the group, printing their names
    int retval;
    fmt::print(stderr, "\nIterating over the group printing names of member objects\n");
    retval = H5Giterate(group, ".", NULL, &get_name, NULL);
    if (retval > 0)
        fmt::print(stderr, "Successful early termination of H5GIterate with retval {}\n", retval);
    else if (retval == 0)
        fmt::print(stderr, "Successful full iteration of H5GIterate with retval {}\n", retval);
    else
        fmt::print(stderr, "Failure during iteration of H5GIterate with retval {}\n", retval);

    // iterate over the objects in the group, checking for grid
    fmt::print(stderr, "\nIterating over the group checking for grid object\n");
    retval = H5Giterate(group, ".", NULL, &check_name, (void*)"grid");
    if (retval > 0)
        fmt::print(stderr, "Successful early termination of H5GIterate with retval {}\n", retval);
    else if (retval == 0)
        fmt::print(stderr, "Successful full iteration of H5GIterate with retval {}\n", retval);
    else
        fmt::print(stderr, "Failure during iteration of H5GIterate with retval {}\n", retval);

    // iterate over the objects in the group, checking for particles
    fmt::print(stderr, "\nIterating over the group checking for particles object\n");
    retval = H5Giterate(group, ".", NULL, &check_name, (void*)"particles");
    if (retval > 0)
        fmt::print(stderr, "Successful early termination of H5GIterate with retval {}\n", retval);
    else if (retval == 0)
        fmt::print(stderr, "Successful full iteration of H5GIterate with retval {}\n", retval);
    else
        fmt::print(stderr, "Failure during iteration of H5GIterate with retval {}\n", retval);

    // iterate over the objects in the group, checking for nonexistent name
    fmt::print(stderr, "\nIterating over the group checking for nonexistent object\n");
    retval = H5Giterate(group, ".", NULL, &check_name, (void*)"nonexistent");
    if (retval > 0)
        fmt::print(stderr, "Successful early termination of H5GIterate with retval {}\n", retval);
    else if (retval == 0)
        fmt::print(stderr, "Successful full iteration of H5GIterate with retval {}\n", retval);
    else
        fmt::print(stderr, "Failure during iteration of H5GIterate with retval {}\n", retval);

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);

    return 0;
}

