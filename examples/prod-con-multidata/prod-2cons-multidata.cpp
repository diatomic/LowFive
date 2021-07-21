#include    <thread>

#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    <dlfcn.h>

#include    "prod-con-multidata.hpp"

int main(int argc, char* argv[])
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
    float                     prod_frac         = 0.333;          // fraction of world ranks in producer
    float                     con1_frac         = 0.333;          // fraction of world ranks in consumer 1
    size_t                    local_npoints     = 100;            // points per block
    std::string               producer_exec     = "./producer-multidata.hx";    // name of producer executable
    std::string               consumer1_exec    = "./consumer1-multidata.hx";   // name of consumer1 executable
    std::string               consumer2_exec    = "./consumer2-multidata.hx";   // name of consumer2 executable


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
        >> Option('n', "number",    local_npoints,  "number of points per block")
        >> Option('b', "blocks",    global_nblocks, "number of blocks")
        >> Option('t', "thread",    threads,        "number of threads")
        >> Option(     "memblks",   mem_blocks,     "number of blocks to keep in memory")
        >> Option(     "prefix",    prefix,         "prefix for external storage")
        >> Option('m', "memory",    metadata,       "build and use in-memory metadata")
        >> Option('f', "file",      passthru,       "write file to disk")
        >> Option(     "p_frac",    prod_frac,      "fraction of world ranks in producer")
        >> Option(     "c_frac",    con1_frac,      "fraction of world ranks in each consumer")
        >> Option('s', "shared",    shared,         "share ranks between producer and consumer (-p ignored)")
        >> Option(     "prod_exec", producer_exec,  "name of producer executable")
        >> Option(     "con1_exec", consumer1_exec, "name of consumer executable")
        >> Option(     "con2_exec", consumer2_exec, "name of consumer executable")
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

    if (metadata && !passthru)
        if (world.rank() == 0)
            fmt::print(stderr, "Using metadata (memory) communication\n");

    if (!metadata && passthru)
        if (world.rank() == 0)
            fmt::print(stderr, "Using passthru (file) communication\n");

    if (metadata && passthru)
        if (world.rank() == 0)
            fmt::print(stderr, "Using metadata (memory) communication and also writing a file\n");

    if (!(metadata + passthru))
    {
        if (world.rank() == 0)
            fmt::print(stderr, "Error: Either metadata or passthru must be enabled. Both cannot be disabled.\n");
        abort();
    }

    // ---  all ranks running workflow runtime system code ---

    size_t global_npoints = global_nblocks * local_npoints;         // all block have same number of points

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * global_nblocks;

    // task ranks: world is producer followed by consumer1 followed by consumer2
    // only relevant for space partitioning (!shared)
    int producer_ranks  = world.size() * prod_frac;
    int consumer1_ranks = world.size() * con1_frac;
    int consumer2_ranks = world.size() - producer_ranks - consumer1_ranks;
    bool producer       = (world.rank() < producer_ranks);
    bool consumer1      = (world.rank() >= producer_ranks && world.rank() < producer_ranks + consumer1_ranks);
    bool consumer2      = (!producer && !consumer1);

    if (!shared && world.rank() == 0)
        fmt::print("space partitioning: producer_ranks: {} consumer1_ranks: {} consumer2_ranks: {}\n",
                producer_ranks, consumer1_ranks, consumer2_ranks);
    if (shared && world.rank() == 0)
        fmt::print("space sharing: producer_ranks = consumer1_ranks = consumer2_ranks = world: {}\n", world.size());

    void* lib_producer = dlopen(producer_exec.c_str(), RTLD_LAZY);
    if (!lib_producer)
        fmt::print(stderr, "Couldn't open producer.hx\n");

    void* lib_consumer1 = dlopen(consumer1_exec.c_str(), RTLD_LAZY);
    if (!lib_consumer1)
        fmt::print(stderr, "Couldn't open consumer1.hx\n");

    void* lib_consumer2 = dlopen(consumer2_exec.c_str(), RTLD_LAZY);
    if (!lib_consumer2)
        fmt::print(stderr, "Couldn't open consumer2.hx\n");

    void* producer_f_ = dlsym(lib_producer, "producer_f");
    if (!producer_f_)
        fmt::print(stderr, "Couldn't find producer_f\n");

    void* consumer1_f_ = dlsym(lib_consumer1, "consumer1_f");
    if (!consumer1_f_)
        fmt::print(stderr, "Couldn't find consumer1_f\n");

    void* consumer2_f_ = dlsym(lib_consumer2, "consumer2_f");
    if (!consumer2_f_)
        fmt::print(stderr, "Couldn't find consumer2_f\n");

    using communicator = diy::mpi::communicator;

    std::mutex exclusive;

    communicator producer_comm, consumer1_comm, consumer2_comm;
    producer_comm.duplicate(world);
    consumer1_comm.duplicate(world);
    consumer2_comm.duplicate(world);

    auto producer_f = [&]()
    {
        ((void (*) (communicator&, communicator, std::mutex&, bool,
                              std::string, int, int,
                              int, int, int, int,
                              Bounds,
                              int, int, size_t)) (producer_f_))(world,
                                                                producer_comm,
                                                                exclusive,
                                                                shared,
                                                                prefix,
                                                                producer_ranks,
                                                                consumer1_ranks,
                                                                metadata,
                                                                passthru,
                                                                threads,
                                                                mem_blocks,
                                                                domain,
                                                                global_nblocks,
                                                                dim,
                                                                local_npoints);
    };

    auto consumer1_f = [&]()
    {
        ((void (*) (communicator&, communicator, std::mutex&, bool,
                              std::string, int,
                              int, int, int, int,
                              Bounds,
                              int, int, size_t)) (consumer1_f_))(world,
                                                                consumer1_comm,
                                                                exclusive,
                                                                shared,
                                                                prefix,
                                                                producer_ranks,
                                                                metadata,
                                                                passthru,
                                                                threads,
                                                                mem_blocks,
                                                                domain,
                                                                con_nblocks,
                                                                dim,
                                                                global_npoints);
    };

    auto consumer2_f = [&]()
    {
        ((void (*) (communicator&, communicator, std::mutex&, bool,
                              std::string, int,
                              int, int, int, int,
                              Bounds,
                              int, int, size_t)) (consumer2_f_))(world,
                                                                consumer2_comm,
                                                                exclusive,
                                                                shared,
                                                                prefix,
                                                                producer_ranks,
                                                                metadata,
                                                                passthru,
                                                                threads,
                                                                mem_blocks,
                                                                domain,
                                                                con_nblocks,
                                                                dim,
                                                                global_npoints);
    };

    if (!shared)
    {
        if (producer)
            producer_f();
        else if (consumer1)
            consumer1_f();
        else
            consumer2_f();
    } else
    {
        auto producer_thread = std::thread(producer_f);
        auto consumer1_thread = std::thread(consumer1_f);
        auto consumer2_thread = std::thread(consumer2_f);

        producer_thread.join();
        consumer1_thread.join();
        consumer2_thread.join();
    }
}