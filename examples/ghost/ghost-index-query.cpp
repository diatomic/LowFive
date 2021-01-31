#include    <diy/master.hpp>
#include    <diy/reduce.hpp>
#include    <diy/partners/swap.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "ghost.hpp"
#include    "index-query.hpp"

static const unsigned DIM = 3;
typedef     PointBlock<DIM>             Block;
typedef     AddPointBlock<DIM>          AddBlock;

int main(int argc, char* argv[])
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv);
    diy::mpi::communicator    world;

    int                       nblocks     = world.size();   // global number of blocks
    size_t                    num_points  = 100;            // points per block
    int                       mem_blocks  = -1;             // all blocks in memory
    int                       threads     = 1;              // no multithreading
    int                       k           = 2;              // radix for k-ary reduction
    std::string               prefix      = "./DIY.XXXXXX"; // for saving block files out of core
    bool                      core        = false;          // in-core or MPI-IO file driver
    int                       ghost       = 0;              // number of ghost points (core - bounds) per side

    // default global data bounds
    Bounds domain { dim };
    domain.min[0] = domain.min[1] = domain.min[2] = 0;
    domain.max[0] = domain.max[1] = domain.max[2] = 100.;

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
        >> Option('n', "number",  num_points,     "number of points per block")
        >> Option('k', "k",       k,              "use k-ary swap")
        >> Option('b', "blocks",  nblocks,        "number of blocks")
        >> Option('t', "thread",  threads,        "number of threads")
        >> Option('m', "memory",  mem_blocks,     "number of blocks to keep in memory")
        >> Option('c', "core",    core,           "whether use in-core file driver or MPI-IO")
        >> Option('g', "ghost",   ghost,          "number of ghost points per side in local grid")
        >> Option(     "prefix",  prefix,         "prefix for external storage")
        ;
    ops
        >> Option('x',  "max-x",  domain.max[0],  "domain max x")
        >> Option('y',  "max-y",  domain.max[1],  "domain max y")
        >> Option('z',  "max-z",  domain.max[2],  "domain max z")
        ;

    bool verbose, help;
    ops
        >> Option('v', "verbose", verbose,  "print the block contents")
        >> Option('h', "help",    help,     "show help")
        ;

    if (!ops.parse(argc,argv) || help)
    {
        if (world.rank() == 0)
        {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]\n";
            std::cout << "Generates random particles in the domain and redistributes them into correct blocks.\n";
            std::cout << ops;
        }
        return 1;
    }

    // diy initialization
    diy::FileStorage          storage(prefix);
    diy::Master               master(world,
                                     threads,
                                     mem_blocks,
                                     &Block::create,
                                     &Block::destroy,
                                     &storage,
                                     &Block::save,
                                     &Block::load);
    AddBlock                  create(master, num_points);
    diy::ContiguousAssigner   assigner(world.size(), nblocks);

    // decompose the domain into blocks
    BoolVector share_face(dim);                         // initializes to all false by default
    BoolVector wrap(dim);                               // initializes to all false by default
    CoordinateVector ghosts(dim);
    for (auto i = 0; i < dim; i++)
        ghosts[i] = ghost;
    diy::RegularDecomposer<Bounds> decomposer(dim, domain, nblocks, share_face, wrap, ghosts);
    decomposer.decompose(world.rank(), assigner, create);


    // Set up file access property list with core or mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (core)
    {
        fmt::print("Using in-core file driver\n");
        H5Pset_fapl_core(plist, 1024 /* grow memory by this incremenet */, 0 /* bool backing_store (actual file) */);
    }
    else
    {
        fmt::print("Using mpi-io file driver\n");
        H5Pset_fapl_mpio(plist, master.communicator(), MPI_INFO_NULL);
    }

    // set up lowfive
    l5::MetadataVOL vol_plugin;
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    /******************/
    /* Write the data */
    /******************/

    // Create a new file using default properties
    hid_t file = H5Fcreate("outfile1.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);

    // Create top-level group
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < DIM; i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // Create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // Create the dataset
    hid_t dset = H5Dcreate2(group, "/group1/grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // test writing an HDF5 file in parallel using HighFive API
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_hdf5(cp, dset); });

    /******************/
    /* Index the data */
    /******************/

    auto* dataset = static_cast<const l5::Dataset*>(vol_plugin.locate("outfile1.h5", "/group1/grid"));
    if (!dataset)
    {
        fmt::print("Dataset not found\n");
        return 1;
    }
    Index index(world, dataset);
    fmt::print("Index created\n");
    index.print();

    /*******************/
    /* "Read" the data */
    /*******************/

    // Create the dataset
    hid_t dset2 = H5Dcreate2(group, "/group1/grid2", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // test writing an HDF5 file in parallel using HighFive API
    // this fills data triplets which we'll use for reading (eventually this
    // will migrate into dataset_read)
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_hdf5(cp, dset2); });

    /******************/
    /* Query the data */
    /******************/

    auto* dataset2 = static_cast<const l5::Dataset*>(vol_plugin.locate("outfile1.h5", "/group1/grid2"));
    if (!dataset2)
    {
        fmt::print("Dataset not found\n");
        return 1;
    }
    index.query(dataset2->data);

    // clean up
    herr_t status;
    status = H5Dclose(dset2);
    status = H5Dclose(dset);
    status = H5Sclose(filespace);
    status = H5Gclose(group);
    status = H5Fclose(file);
    status = H5Pclose(plist);
}
