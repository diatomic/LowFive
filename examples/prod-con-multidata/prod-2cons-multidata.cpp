#include    <stddef.h>

#include    <thread>

#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "load-task.hpp"

#include    "prod-con-multidata.hpp"

herr_t
fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
}

int main(int argc, char* argv[])
{
    // logging output from LowFive (comment out for no output)
//     l5::create_logger("trace");         // generates the most output
//     l5::create_logger("debug");         // generates less output
//     l5::create_logger("info");          // generates even less output (prints metadata tree)

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
    float                     prod_frac         = 1.0 / 3.0;      // fraction of world ranks in producer
    float                     con1_frac         = 1.0 / 3.0;      // fraction of world ranks in consumer 1
    size_t                    local_npoints     = 100;            // points per block
    std::string               producer_exec     = "./producer-multidata.hx";    // name of producer executable
    std::string               consumer1_exec    = "./consumer1-multidata.hx";   // name of consumer1 executable
    std::string               consumer2_exec    = "./consumer2-multidata.hx";   // name of consumer2 executable
    std::string               log_level         = "";

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
        >> Option(     "c1_frac",   con1_frac,      "fraction of world ranks in consumer1")
        >> Option('s', "shared",    shared,         "share ranks between producer and consumer (-p ignored)")
        >> Option(     "prod_exec", producer_exec,  "name of producer executable")
        >> Option(     "con1_exec", consumer1_exec, "name of consumer executable")
        >> Option(     "con2_exec", consumer2_exec, "name of consumer executable")
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

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * global_nblocks;

    // task ranks: world is producer followed by consumer1 followed by consumer2
    // only relevant for space partitioning (!shared)
    int producer_ranks  = world.size() * prod_frac;
    int consumer1_ranks = world.size() * con1_frac;
    int consumer2_ranks = world.size() - producer_ranks - consumer1_ranks;
    int task;                       // enum: producer_task, consumer1_task, consumer2_task
    if (world.rank() < producer_ranks)
        task = producer_task;
    else if (world.rank() >= producer_ranks && world.rank() < producer_ranks + consumer1_ranks)
        task = consumer1_task;
    else
        task = consumer2_task;

    if (!shared && world.rank() == 0)
        fmt::print("space partitioning: producer_ranks: {} consumer1_ranks: {} consumer2_ranks: {}\n",
                producer_ranks, consumer1_ranks, consumer2_ranks);
    if (shared && world.rank() == 0)
        fmt::print("space sharing: producer_ranks = consumer1_ranks = consumer2_ranks = world: {}\n", world.size());

    // Load and validate every task before any rank splits communicators.
    void* lib_producer = nullptr;
    void* lib_consumer1 = nullptr;
    void* lib_consumer2 = nullptr;
    void* producer_f_ = lowfive_examples::load_task(producer_exec, "producer_f", lib_producer, world.rank());
    void* consumer1_f_ = lowfive_examples::load_task(consumer1_exec, "consumer1_f", lib_consumer1, world.rank());
    void* consumer2_f_ = lowfive_examples::load_task(consumer2_exec, "consumer2_f", lib_consumer2, world.rank());
    int load_failed = !producer_f_ || !consumer1_f_ || !consumer2_f_;
    int any_load_failed = 0;
    MPI_Allreduce(&load_failed, &any_load_failed, 1, MPI_INT, MPI_MAX, world);
    if (any_load_failed)
    {
        // No task entry point has run; discard the handles acquired for startup.
        if (lib_consumer2) dlclose(lib_consumer2);
        if (lib_consumer1) dlclose(lib_consumer1);
        if (lib_producer) dlclose(lib_producer);
        return 1;
    }

    // set HDF5 error handler
    H5Eset_auto(H5E_DEFAULT, fail_on_hdf5_error, NULL);

    // communicator management
    using communicator = MPI_Comm;
    MPI_Comm intercomm_, intercomm1_, intercomm2_;
    std::vector<communicator> producer_intercomms, consumer1_intercomms, consumer2_intercomms;
    communicator p_c1_intercomm, p_c2_intercomm;
    communicator local;
    communicator producer_comm, consumer1_comm, consumer2_comm;

    if (shared)
    {
        MPI_Comm_dup(world, &producer_comm);
        MPI_Comm_dup(world, &consumer1_comm);
        MPI_Comm_dup(world, &consumer2_comm);

        MPI_Comm_dup(world, &p_c1_intercomm);
        MPI_Comm_dup(world, &p_c2_intercomm);

        producer_intercomms.push_back(p_c1_intercomm);
        producer_intercomms.push_back(p_c2_intercomm);

        consumer1_intercomms.push_back(p_c1_intercomm);
        consumer2_intercomms.push_back(p_c2_intercomm);
    }
    else
    {
        MPI_Comm_split(world, task, 0, &local);

        if (task == producer_task)
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer_ranks, /* tag = */ 0, &intercomm1_);
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer_ranks + consumer1_ranks, /* tag = */ 0, &intercomm2_);

            producer_intercomms.push_back(communicator(intercomm1_));
            producer_intercomms.push_back(communicator(intercomm2_));

            producer_comm = local;
        }
        else
        {
            MPI_Intercomm_create(local, 0, world, /* remote_leader = */ 0, /* tag = */ 0, &intercomm_);

            if (task == consumer1_task)
            {
                consumer1_comm = local;
                consumer1_intercomms.push_back(communicator(intercomm_));
            } else // task == consumer2_task
            {
                consumer2_comm = local;
                consumer2_intercomms.push_back(communicator(intercomm_));
            }
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

    auto consumer1_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    int)) (consumer1_f_))(consumer1_comm,
                                          consumer1_intercomms,
                                          exclusive,
                                          shared,
                                          prefix,
                                          metadata,
                                          passthru,
                                          threads,
                                          mem_blocks,
                                          con_nblocks);
    };

    auto consumer2_f = [&]()
    {
        ((void (*) (communicator&, const std::vector<communicator>&,
                    std::mutex&, bool,
                    std::string,
                    int, int, int, int,
                    int)) (consumer2_f_))(consumer2_comm,
                                          consumer2_intercomms,
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
        if (task == producer_task)
            producer_f();
        else if (task == consumer1_task)
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
