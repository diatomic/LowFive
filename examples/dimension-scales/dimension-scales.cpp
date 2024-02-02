#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "../prod-con-multidata/prod-con-multidata.hpp"

#include    <hdf5.h>
#include    <H5DSpublic.h>

using communicator = diy::mpi::communicator;

herr_t fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
}

void nonneg(herr_t retval, const char* msg)
{
    if (retval < 0)
    {
        fmt::print(stderr, "Error executing {}\n", msg);
        abort();
    }
}

void zero(herr_t retval, const char* msg)
{
    if (retval)
    {
        fmt::print(stderr, "Error executing {}\n", msg);
        abort();
    }
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

//    l5::VOLBase& vol_plugin = l5::VOLBase::create_vol_base();

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
    std::cerr << "H5Fcreate OK" << std::endl;
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    std::cerr << "H5Gcreate OK" << std::endl;

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < dim; i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    std::cerr << "H5Screate_simple OK" << std::endl;
    // create the grid dataset with default properties
    hid_t dset = H5Dcreate(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::cerr << "H5Dcreate OK" << std::endl;
    // write the grid data
    prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_grid(cp, dset); });

    // dimension scales
    // make a 1-d array dataset for the scale for each dimension of the grid data
    // the scale is a vector of values, each associated with each point in the grid, eg, its physical space position
    // although HDF5 makes no restrictions on how dimension scales are used, that's a typical scenario
    // the dimension scale of another dataset is itself a dataset that needs to be created first
    std::vector<hid_t>              scale_spaces(DIM);
    std::vector<hid_t>              scale_dsets(DIM);
    std::vector<std::vector<float>> scale_data(DIM);
    char dset_name[256];
    char dim_name[256];
    herr_t retval;
    const int loop_iters = 2;
    for (int i = 0; i < loop_iters; i++)      // for all dimensions
    {
        // dataspace for the scale dataset
        scale_spaces[i] = H5Screate_simple(1, &domain_cnts[i], NULL);

        std::cerr << "H5Screate_simple for scale dataset OK" << std::endl;
        // dataset for the scale
        snprintf(dset_name, 256, "grid_scale%d", i);
        scale_dsets[i] = H5Dcreate(group, dset_name, H5T_NATIVE_FLOAT, scale_spaces[i], H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        std::cerr << "H5Dcreate_simple for scale dataset OK" << std::endl;
        scale_data[i].resize(domain_cnts[i]);
        for (int j = 0; j < domain_cnts[i]; j++)
            scale_data[i][j] = j;
        retval = H5Dwrite(scale_dsets[i], H5T_NATIVE_FLOAT, scale_spaces[i], scale_spaces[i], H5P_DEFAULT, &scale_data[i][0]); nonneg(retval, "H5Dwrite");
        std::cerr << "H5Dwrite OK" << std::endl;

        // create a dimension scale out of a dataset
        snprintf(dim_name, 256, "dim%d", i);
        retval = H5DSset_scale(scale_dsets[i], dim_name); zero(retval, "H5DSset_scale");

        // check the scale setting
        fmt::print(stderr, "After setting scale, checking if {} is a dimension scale: {}\n", dim_name, H5DSis_scale(scale_dsets[i]));

        // attach a dimension scale to the grid dataset
        retval = H5DSattach_scale(dset, scale_dsets[i], i); zero(retval, "H5DSattach_scale");

        auto file_cnt = H5Iget_ref(72057594037927936);
        std::cerr << "After attach_scale, file_cnt = " << file_cnt << std::endl;

        // check the scale attaching
        fmt::print(stderr, "After attaching scale, checking how many scales are attached to grid dimension {}: {}\n",
                i, H5DSget_num_scales(dset, i));
        // for some reason the following seg-faults in passthru mode
//         fmt::print(stderr, "After attaching scale, checking if {} is attached to grid dimension {}: {}\n",
//                 dim_name, i, H5DSis_attached(dset, scale_dsets[i], i));
    }


    auto file_cnt = H5Iget_ref(72057594037927936);
    std::cerr << "Attach scale LOOP DONE file_cnt = " << file_cnt << std::endl;

    // clean up
    for (int i = 0; i < loop_iters; i++)
    {
        H5Dclose(scale_dsets[i]);
        H5Sclose(scale_spaces[i]);
    }
    std::cerr << "CLOSELOOP DONE" << std::endl;

    H5Dclose(dset);
    H5Sclose(filespace);
    H5Gclose(group);
    std::cerr << "CLOSING FILE..." << std::endl;
    H5Fclose(file);
    std::cerr << "CLOSING FILE OK" << std::endl;
    H5Pclose(plist);

    return 0;
}

