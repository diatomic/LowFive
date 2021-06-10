#include    <thread>

#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    <dlfcn.h>

#include    "prod-con.hpp"

int main(int argc, char* argv[])
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv, MPI_THREAD_MULTIPLE);
    diy::mpi::communicator    world;

    int                       nblocks     = world.size();   // global number of blocks
    int                       mem_blocks  = -1;             // all blocks in memory
    int                       threads     = 1;              // no multithreading
    std::string               prefix      = "./DIY.XXXXXX"; // for saving block files out of core
    // opts does not handle bool correctly, using int instead
    int                       metadata    = 1;              // build in-memory metadata
    int                       passthru    = 0;              // write file to disk
    float                     prod_frac   = 0.5;            // fraction of world ranks in producer
    bool                      shared      = false;          // producer and consumer run on the same ranks

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
        >> Option('p', "p_frac",  prod_frac,      "fraction of world ranks in producer")
        >> Option('s', "shared",  shared,         "share ranks between producer and consumer (-p ignored)")
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

    if (!(metadata + passthru))
    {
        fmt::print(stderr, "Error: Either metadata or passthru must be enabled. Both cannot be disabled.\n");
        abort();
    }

    // ---  all ranks running workflow runtime system code ---

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * nblocks;

    int producer_ranks = world.size() * prod_frac;
    bool producer = world.rank() < producer_ranks;
    fmt::print("producer_ranks: {}\n", producer_ranks);

    void* lib_producer = dlopen("./producer.hx", RTLD_LAZY);
    if (!lib_producer)
        fmt::print(stderr, "Couldn't open producer.hx\n");

    void* lib_consumer = dlopen("./consumer.hx", RTLD_LAZY);
    if (!lib_consumer)
        fmt::print(stderr, "Couldn't open consumer.hx\n");

    void* producer_f_ = dlsym(lib_producer, "producer_f");
    if (!producer_f_)
        fmt::print(stderr, "Couldn't find producer_f\n");
    void* consumer_f_ = dlsym(lib_consumer, "consumer_f");
    if (!consumer_f_)
        fmt::print(stderr, "Couldn't find consumer_f\n");

    using communicator = diy::mpi::communicator;

    std::mutex exclusive;

    communicator producer_comm, consumer_comm;
    producer_comm.duplicate(world);
    consumer_comm.duplicate(world);

    auto producer_f = [&]()
    {
        ((void (*) (communicator&, communicator, std::mutex&, bool,
                              std::string, int,
                              int, int, int, int,
                              Bounds,
                              int, int)) (producer_f_))(world, producer_comm, exclusive, shared, prefix, producer_ranks, metadata, passthru, threads, mem_blocks, domain, nblocks, dim);
    };

    auto consumer_f = [&]()
    {
        ((void (*) (communicator&, communicator, std::mutex&, bool,
                              std::string, int,
                              int, int, int, int,
                              Bounds,
                              int, int)) (consumer_f_))(world, consumer_comm, exclusive, shared, prefix, producer_ranks, metadata, passthru, threads, mem_blocks, domain, con_nblocks, dim);
    };

    if (!shared)
    {
        if (producer)
            producer_f();
        else
            consumer_f();
    } else
    {
        auto producer_thread = std::thread(producer_f);
        auto consumer_thread = std::thread(consumer_f);

        producer_thread.join();
        consumer_thread.join();
    }
}
