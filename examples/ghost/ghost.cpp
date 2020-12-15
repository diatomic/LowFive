// using our Vol wrapper

#include    <cmath>

#include    <diy/master.hpp>
#include    <diy/reduce.hpp>
#include    <diy/partners/swap.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "ghost.hpp"

static const unsigned DIM = 3;
typedef     PointBlock<DIM>             Block;
typedef     AddPointBlock<DIM>          AddBlock;

int main(int argc, char* argv[])
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv); // equivalent of MPI_Init(argc, argv)/MPI_Finalize()
    diy::mpi::communicator    world;           // equivalent of MPI_COMM_WORLD

    int                       nblocks     = world.size();   // global number of blocks
    size_t                    num_points  = 100;            // points per block
    int                       mem_blocks  = -1;             // all blocks in memory
    int                       threads     = -1;             // no multithreading
    int                       k           = 2;              // radix for k-ary reduction
    std::string               prefix      = "./DIY.XXXXXX"; // for saving block files out of core
    bool                      core        = false;          // in-core or MPI-IO file driver
    int                       ghost       = 0;              // number of ghost points (core - bounds) per side

    // set some global data bounds (defaults set before option parsing)
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
    diy::FileStorage          storage(prefix);            // used for blocks moved out of core
    diy::Master               master(world,               // top-level diy object
                                     threads,
                                     mem_blocks,
                                     &Block::create,
                                     &Block::destroy,
                                     &storage,
                                     &Block::save,
                                     &Block::load);
    AddBlock                  create(master, num_points); // object for adding new blocks to master

    // choice of contiguous or round robin assigner
    diy::ContiguousAssigner   assigner(world.size(), nblocks);
    //diy::RoundRobinAssigner   assigner(world.size(), nblocks);

    // decompose the domain into blocks
    BoolVector share_face(dim);                         // initializes to all false by default
    BoolVector wrap(dim);                               // initializes to all false by default
    CoordinateVector ghosts(dim);
    for (auto i = 0; i < dim; i++)
        ghosts[i] = ghost;
    diy::RegularDecomposer<Bounds> decomposer(dim, domain, nblocks, share_face, wrap, ghosts);
    decomposer.decompose(world.rank(), assigner, create);

    // test writing an HDF5 file in parallel using HighFive API
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_hdf5(cp, core); });
}
