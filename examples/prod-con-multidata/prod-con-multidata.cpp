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
    float                     prod_frac         = 0.5;            // fraction of world ranks in producer
    size_t                    local_npoints     = 100;            // points per block
    std::string               producer_exec     = "./producer-multidata.hx";    // name of producer executable
    std::string               consumer_exec     = "./consumer-multidata.hx";    // name of consumer executable


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
        >> Option('p', "p_frac",    prod_frac,      "fraction of world ranks in producer")
        >> Option('s', "shared",    shared,         "share ranks between producer and consumer (-p ignored)")
        >> Option('r', "prod_exec", producer_exec,  "name of producer executable")
        >> Option('c', "con_exec",  consumer_exec,  "name of consumer executable")
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

    size_t global_npoints = global_nblocks * local_npoints;         // all block have same number of points

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * global_nblocks;

    int producer_ranks = world.size() * prod_frac;
    bool producer = world.rank() < producer_ranks;

    if (!shared && world.rank() == 0)
        fmt::print("space partitioning: producer_ranks: {} consumer_ranks: {}\n", producer_ranks, world.size() - producer_ranks);
    if (shared && world.rank() == 0)
        fmt::print("space sharing: producer_ranks = consumer_ranks = world: {}\n", world.size());

    // load tasks
    void* lib_producer = dlopen(producer_exec.c_str(), RTLD_LAZY);
    if (!lib_producer)
        fmt::print(stderr, "Couldn't open producer.hx\n");

    void* lib_consumer = dlopen(consumer_exec.c_str(), RTLD_LAZY);
    if (!lib_consumer)
        fmt::print(stderr, "Couldn't open consumer.hx\n");

    void* producer_f_ = dlsym(lib_producer, "producer_f");
    if (!producer_f_)
        fmt::print(stderr, "Couldn't find producer_f\n");
    void* consumer_f_ = dlsym(lib_consumer, "consumer_f");
    if (!consumer_f_)
        fmt::print(stderr, "Couldn't find consumer_f\n");

    // communicator management
    using communicator = diy::mpi::communicator;
    MPI_Comm intercomm_;
    std::vector<communicator> producer_intercomms, consumer_intercomms;
    communicator p_c_intercomm;
    communicator local;
    communicator producer_comm, consumer_comm;
    producer_comm.duplicate(world);
    consumer_comm.duplicate(world);

    if (shared)
    {
        producer_comm.duplicate(world);
        consumer_comm.duplicate(world);
        p_c_intercomm.duplicate(world);
        producer_intercomms.push_back(p_c_intercomm);
        consumer_intercomms.push_back(p_c_intercomm);
    }
    else
    {
        local = world.split(producer);

        if (producer)
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer_ranks, /* tag = */ 0, &intercomm_);
            producer_intercomms.push_back(communicator(intercomm_));
            producer_comm = local;
        }
        else
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ 0, /* tag = */ 0, &intercomm_);
            consumer_intercomms.push_back(communicator(intercomm_));
            consumer_comm = local;
        }
    }

    std::mutex exclusive;

    // declare lambdas for the tasks

    auto producer_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    Bounds,
                    int, int, size_t)) (producer_f_))(producer_comm,
                                                      producer_intercomms,
                                                      exclusive,
                                                      shared,
                                                      prefix,
                                                      metadata,
                                                      passthru,
                                                      threads,
                                                      mem_blocks,
                                                      domain,
                                                      global_nblocks,
                                                      dim,
                                                      local_npoints);
    };

    auto consumer_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    int))(consumer_f_))(consumer_comm,
                                        consumer_intercomms,
                                        exclusive,
                                        shared,
                                        prefix,
                                        metadata,
                                        passthru,
                                        threads,
                                        mem_blocks,
                                        con_nblocks);
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
