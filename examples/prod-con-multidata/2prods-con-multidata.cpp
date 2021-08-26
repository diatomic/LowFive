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
    float                     prod1_frac        = 0.333;          // fraction of world ranks in producer1
    float                     prod2_frac        = 0.333;          // fraction of world ranks in producer2
    size_t                    local_npoints     = 100;            // points per block
    std::string               producer1_exec    = "./producer1-multidata.hx";   // name of producer1 executable
    std::string               producer2_exec    = "./producer2-multidata.hx";   // name of producer2 executable
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
        >> Option(     "p1_frac",   prod1_frac,     "fraction of world ranks in producer1")
        >> Option(     "p2_frac",   prod2_frac,     "fraction of world ranks in producer2")
        >> Option('s', "shared",    shared,         "share ranks between producer and consumer (-p ignored)")
        >> Option(     "prod1_exec",producer1_exec, "name of producer1 executable")
        >> Option(     "prod2_exec",producer2_exec, "name of producer2 executable")
        >> Option(     "con_exec",  consumer_exec,  "name of consumer executable")
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

    // task ranks: world is producer followed by consumer1 followed by consumer2
    // only relevant for space partitioning (!shared)
    int producer1_ranks = world.size() * prod1_frac;
    int producer2_ranks = world.size() * prod2_frac;
    int consumer_ranks  = world.size() - producer1_ranks - producer2_ranks;
    int task;                       // enum: producer1_task, producer2_task, consumer_task
    if (world.rank() < producer1_ranks)
        task = producer1_task;
    else if (world.rank() >= producer1_ranks && world.rank() < producer1_ranks + producer2_ranks)
        task = producer2_task;
    else
        task = consumer_task;

    if (!shared && world.rank() == 0)
        fmt::print("space partitioning: producer1_ranks: {} producer2_ranks: {} consumer_ranks: {}\n",
                producer1_ranks, producer2_ranks, consumer_ranks);
    if (shared && world.rank() == 0)
        fmt::print("space sharing: producer1_ranks = producer2_ranks = consumer_ranks = world: {}\n", world.size());

    // load tasks
    void* lib_producer1 = dlopen(producer1_exec.c_str(), RTLD_LAZY);
    if (!lib_producer1)
        fmt::print(stderr, "Couldn't open producer1.hx\n");

    void* lib_producer2 = dlopen(producer2_exec.c_str(), RTLD_LAZY);
    if (!lib_producer2)
        fmt::print(stderr, "Couldn't open producer2.hx\n");

    void* lib_consumer = dlopen(consumer_exec.c_str(), RTLD_LAZY);
    if (!lib_consumer)
        fmt::print(stderr, "Couldn't open consumer.hx\n");

    void* producer1_f_ = dlsym(lib_producer1, "producer1_f");
    if (!producer1_f_)
        fmt::print(stderr, "Couldn't find producer1_f\n");

    void* producer2_f_ = dlsym(lib_producer2, "producer2_f");
    if (!producer2_f_)
        fmt::print(stderr, "Couldn't find producer2_f\n");

    void* consumer_f_ = dlsym(lib_consumer, "consumer_f");
    if (!consumer_f_)
        fmt::print(stderr, "Couldn't find consumer_f\n");

    // communicator management
    using communicator = diy::mpi::communicator;
    MPI_Comm intercomm_, intercomm1_, intercomm2_;
    std::vector<communicator> producer1_intercomms, producer2_intercomms, consumer_intercomms;
    communicator p1_c_intercomm, p2_c_intercomm;
    communicator local;
    communicator producer1_comm, producer2_comm, consumer_comm;

    if (shared)
    {
        producer1_comm.duplicate(world);
        producer2_comm.duplicate(world);
        consumer_comm.duplicate(world);

        p1_c_intercomm.duplicate(world);
        p2_c_intercomm.duplicate(world);

        producer1_intercomms.push_back(p1_c_intercomm);
        producer2_intercomms.push_back(p2_c_intercomm);

        consumer_intercomms.push_back(p1_c_intercomm);
        consumer_intercomms.push_back(p2_c_intercomm);
    }
    else
    {
        local = world.split(task);

        if (task == producer1_task || task == producer2_task)
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer1_ranks + producer2_ranks, /* tag = */ 0, &intercomm_);
            if (task == producer1_task)
            {
                producer1_intercomms.push_back(communicator(intercomm_));
                producer1_comm = local;
            }
            else    // task = producer2_task
            {
                producer2_intercomms.push_back(communicator(intercomm_));
                producer2_comm = local;
            }
        }
        else        // task = consumer_task
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ 0, /* tag = */ 0, &intercomm1_);
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer1_ranks, /* tag = */ 0, &intercomm2_);
            consumer_comm = local;
            consumer_intercomms.push_back(communicator(intercomm1_));
            consumer_intercomms.push_back(communicator(intercomm2_));
        }
    }

    std::mutex exclusive;

    // declare lambdas for the tasks

    auto producer1_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    Bounds,
                    int, int, size_t)) (producer1_f_))(producer1_comm,
                                                       producer1_intercomms,
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

    auto producer2_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    Bounds,
                    int, int, size_t)) (producer2_f_))(producer2_comm,
                                                       producer2_intercomms,
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

    // run the task to which this rank belongs
    if (!shared)
    {
        if (task == producer1_task)
            producer1_f();
        else if (task == producer2_task)
            producer2_f();
        else
            consumer_f();
    } else
    {
        auto producer1_thread = std::thread(producer1_f);
        auto producer2_thread = std::thread(producer2_f);
        auto consumer_thread = std::thread(consumer_f);

        producer1_thread.join();
        producer2_thread.join();
        consumer_thread.join();
    }
}
