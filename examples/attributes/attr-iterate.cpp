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
    std::string               log_level         = "";

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

    // set location patterns
    LowFive::LocationPattern all { "outfile.h5", "*"};
    LowFive::LocationPattern grid { "outfile.h5", "/group1/grid"};
    LowFive::LocationPattern particles { "outfile.h5", "/group1/particles"};

    // create the vol plugin
    l5::MetadataVOL vol_plugin;
    if (metadata)
    {
        //vol_plugin.memory.push_back(all);
        vol_plugin.memory.push_back(grid);
        vol_plugin.memory.push_back(particles);
    }
    if (passthru)
    {
        //vol_plugin.passthru.push_back(all);
        vol_plugin.passthru.push_back(grid);
        vol_plugin.passthru.push_back(particles);
    }
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
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // get info back about the group
    H5G_info_t group_info;
    H5Gget_info(group, &group_info);

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

    // add some attributes to the grid dataset
    hid_t attr_space = H5Screate_simple(1, &domain_cnts[0], NULL);
    hid_t attr1 = H5Acreate(dset, "attr1", H5T_IEEE_F32LE, attr_space, H5P_DEFAULT, H5P_DEFAULT);
    hid_t attr2 = H5Acreate(dset, "attr2", H5T_C_S1, attr_space, H5P_DEFAULT, H5P_DEFAULT);

    // iterate through the attributes of the grid dataset, printing their names
    // TODO: currently LowFive ignores the iteration order, increment direction, and current index
    // it just blindly goes through all the attributes in the order they were created
    hsize_t n = 0;
    H5Aiterate(dset, H5_INDEX_CRT_ORDER, H5_ITER_INC, &n, &iter_op, NULL);

    // get info back about the dataset
    H5O_info_t oinfo;
    H5Oget_info(dset, &oinfo, H5O_INFO_ALL);

    // dimension scale example
    // make a 1-d array dataset that will be the scale for the 0th dimension of the grid data
    // in this example, the scale is a vector of values, each associated with each point in the grid, eg, its physical space position
    // although HDF5 makes no restrictions on how dimension scales are used, that's a typical scenario
    // thie example only assigns a scale to one dimension, which is valid; other dimensions could be done similarly
    // the dimension scale of another dataset is itself a dataset that needs to be created first
    hid_t scale_space = H5Screate_simple(1, &domain_cnts[0], NULL);
    hid_t scale = H5Dcreate2(group, "grid_scale", H5T_IEEE_F32LE, scale_space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    std::vector<float> scale_data(domain_cnts[0]);
    for (auto i = 0; i < scale_data.size(); i++)
        scale_data[i] = i * 10;
    H5Dwrite(scale, H5T_NATIVE_FLOAT, scale_space, scale_space, H5P_DEFAULT, &scale_data[0]);

    // now attach the dimension scale dataset to the original grid dataset
    H5DSset_scale(dset, "scale");
    H5DSattach_scale(dset, scale, 0);

    // clean up
    H5Sclose(attr_space);
    H5Dclose(scale);
    H5Sclose(scale_space);
    H5Aclose(attr1);
    H5Aclose(attr2);
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

    // clean up
    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    H5Fclose(file);
    H5Pclose(plist);

    return 0;
}

