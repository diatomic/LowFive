#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "m-to-n.hpp"

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
    // opts does not handle bool correctly, using int instead
    int                       metadata    = 1;              // build in-memory metadata
    int                       passthru    = 0;              // write file to disk

    // default global data bounds
    Bounds domain { dim };
    for (auto i = 0; i < dim; i++)
    {
        domain.min[i] = 0;
        domain.max[i] = 10;
    }

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
        >> Option('b', "blocks",  nblocks,        "number of blocks")
        >> Option('t', "thread",  threads,        "number of threads")
        >> Option(     "memblks", mem_blocks,     "number of blocks to keep in memory")
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
    diy::RegularDecomposer<Bounds> decomposer(dim, domain, nblocks);
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

    // prepare for reading
    hid_t file1, dset1;

    // if an actual file is being written, close it and re-open to be sure it is flushed to disk
    H5Dclose(dset);
    if (passthru)
    {
        // clean up
        H5Sclose(filespace);
        H5Gclose(group);
        H5Fclose(file);

        // re-open
        file1 = H5Fopen("outfile.h5", H5F_ACC_RDWR, plist);
        dset1 = H5Dopen(file1, "/group1/grid", H5P_DEFAULT);
    }
    // if in-memory metadata, just open a new dataset
    // (unnecessary in the same code, but demonstrates how a consumer would open an existing dataset)
    else
        dset1 = H5Dopen(file, "/group1/grid", H5P_DEFAULT);

    // second master for reading the data with 2X the number of blocks in each dim.
    int read_nblocks = pow(2, dim) * nblocks;
    diy::Master               read_master(world,
                                     threads,
                                     mem_blocks,
                                     &Block::create,
                                     &Block::destroy,
                                     &storage,
                                     &Block::save,
                                     &Block::load);
    AddBlock                  read_create(read_master);
    diy::ContiguousAssigner   read_assigner(world.size(), read_nblocks);
    diy::RegularDecomposer<Bounds> read_decomposer(dim, domain, read_nblocks);
    read_decomposer.decompose(world.rank(), read_assigner, read_create);

    // read the data
    read_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->read_block_hdf5(cp, dset1); });

    // clean up
    H5Dclose(dset1);
    if (passthru)
        H5Fclose(file1);
    else
    {
        H5Sclose(filespace);
        H5Gclose(group);
        H5Fclose(file);
    }
    H5Pclose(plist);
}
