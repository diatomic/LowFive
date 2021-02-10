#include    <diy/master.hpp>
#include    <diy/reduce.hpp>
#include    <diy/partners/swap.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "ghost-index-query.hpp"

static const unsigned DIM = 3;
typedef     PointBlock<DIM>             Block;
typedef     AddPointBlock<DIM>          AddBlock;

int main(int argc, char* argv[])
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv);
    diy::mpi::communicator    world;

    int                       nblocks     = world.size();   // global number of blocks
    int                       mem_blocks  = -1;             // all blocks in memory
    int                       threads     = 1;              // no multithreading
    std::string               prefix      = "./DIY.XXXXXX"; // for saving block files out of core
    int                       ghost       = 0;              // number of ghost points (core - bounds) per side
    // opts does not handle bool correctly, using int instead
    int                       metadata    = 1;              // build in-memory metadata
    int                       passthru    = 0;              // write file to disk

    // default global data bounds
    Bounds domain { dim };
    for (auto i = 0; i < dim; i++)
    {
        domain.min[i] = 0;
        domain.max[i] = 5;
    }

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
        >> Option('b', "blocks",  nblocks,        "number of blocks")
        >> Option('t', "thread",  threads,        "number of threads")
        >> Option(     "memblks", mem_blocks,     "number of blocks to keep in memory")
        >> Option('g', "ghost",   ghost,          "number of ghost points per side in local grid")
        >> Option(     "prefix",  prefix,         "prefix for external storage")
        >> Option('m', "memory",  metadata,       "build and use in-memory metadata")
        >> Option('f', "file",    passthru,       "write file to disk")
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
    AddBlock                  create(master);
    diy::ContiguousAssigner   assigner(world.size(), nblocks);

    // decompose the domain into blocks
    BoolVector share_face(dim);                         // initializes to all false by default
    BoolVector wrap(dim);                               // initializes to all false by default
    CoordinateVector ghosts(dim);
    for (auto i = 0; i < dim; i++)
        ghosts[i] = ghost;
    diy::RegularDecomposer<Bounds> decomposer(dim, domain, nblocks, share_face, wrap, ghosts);
    decomposer.decompose(world.rank(), assigner, create);

    // Set up file access property list with mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(plist, master.communicator(), MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(world, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // create a new file using default properties
    hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);

    // create top-level group
    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    std::vector<hsize_t> domain_cnts(DIM);
    for (auto i = 0; i < DIM; i++)
        domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

    // create the file data space for the global grid
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

    // create the dataset
    hid_t dset = H5Dcreate2(group, "/group1/grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // write the data
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_hdf5(cp, dset); });

    // read the data
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_hdf5(cp, dset); });

    // clean up
    herr_t status;
    status = H5Dclose(dset);
    status = H5Sclose(filespace);
    status = H5Gclose(group);
    status = H5Fclose(file);
    status = H5Pclose(plist);
}
