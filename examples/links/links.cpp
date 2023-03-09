#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "../prod-con-multidata/prod-con-multidata.hpp"

#include    <H5DSpublic.h>

using communicator = diy::mpi::communicator;

// user-define operator over attributes
// returns 0: keep iterating, 1: intentional early termination, -1: failure, early termination
herr_t iter_op(
        hid_t               location_id,
        const char          *attr_name,
        const H5A_info_t    *ainfo,
        void *              op_data)
{
    fmt::print(stderr, "Location type: {}\n", H5Iget_type(location_id));

    // in this example, the operator simply prints the attribute name
    fmt::print(stderr, "Iterating over attributes: name = {}\n", attr_name);
    return 0;
}

herr_t fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
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
    size_t                    local_num_points  = 20;            // points per block

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

    // set HDF5 error handler
    H5Eset_auto(H5E_DEFAULT, fail_on_hdf5_error, NULL);

    // set location patterns
    LowFive::LocationPattern all { "outfile.h5", "*"};
    LowFive::LocationPattern none { "outfile.h5", ""};

    // create the vol plugin
    //l5::VOLBase vol_plugin;
    l5::MetadataVOL& vol_plugin = l5::MetadataVOL::create_MetadataVOL();
    if (metadata)
        vol_plugin.memory.push_back(all);
    if (passthru)
        vol_plugin.passthru.push_back(all);

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
    hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    hid_t group1 = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < static_cast<decltype(i)>(DIM); i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the grid dataset with default properties
    hid_t dset = H5Dcreate2(group1, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the grid data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_grid(cp, dset); });

    // add some attributes to the grid dataset
    hid_t attr_space = H5Screate_simple(1, &domain_cnts[0], NULL);
    hid_t attr1 = H5Acreate(dset, "attr1", H5T_IEEE_F32LE, attr_space, H5P_DEFAULT, H5P_DEFAULT);
    hid_t attr2 = H5Acreate(dset, "attr2", H5T_C_S1, attr_space, H5P_DEFAULT, H5P_DEFAULT);
    H5Sclose(attr_space);

    vol_plugin.print_files();

    // create a hard link to the grid
    H5Lcreate_hard(file, "/group1/grid", file, "/group1/grid_link", H5P_DEFAULT, H5P_DEFAULT);

    // clean up
    H5Aclose(attr1);
    H5Aclose(attr2);
    H5Dclose(dset);
    H5Sclose(filespace);

    // create a group for the particles
    hid_t group2 = H5Gcreate(file, "/group2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // create the file data space for the particles
    domain_cnts[0]  = global_num_points;
    domain_cnts[1]  = DIM;
    filespace = H5Screate_simple(2, &domain_cnts[0], NULL);

    // create the particle dataset with default properties
    dset = H5Dcreate2(group2, "particles", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the particle data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_points(cp, dset, global_nblocks); });

    // create a soft link in group 1 to the particles that are physically in group 2
    // and vice versa, a soft link in group 2 to the grid that is physically in group 1
    H5Lcreate_soft("/group2/particles", group1, "particles_link", H5P_DEFAULT, H5P_DEFAULT);
    H5Lcreate_soft("/group1/grid", group2, "grid_link", H5P_DEFAULT, H5P_DEFAULT);

    // make a copy of one soft link and move the other soft link
    H5Lcopy(group1, "particles_link", group1, "copy_of_particles_link", H5P_DEFAULT, H5P_DEFAULT);
    H5Lmove(group2, "grid_link", group2, "moved_grid_link", H5P_DEFAULT, H5P_DEFAULT);

    // delete the copy and move the other link back
    H5Ldelete(group1, "copy_of_particles_link", H5P_DEFAULT);
    H5Lmove(group2, "moved_grid_link", group2, "grid_link", H5P_DEFAULT, H5P_DEFAULT);

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group1);
    H5Gclose(group2);
    H5Fclose(file);
    H5Pclose(plist);

    return 0;
}

