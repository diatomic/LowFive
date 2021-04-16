#include    <diy/master.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    "prod-con.hpp"

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
    float                     prod_frac   = 0.5;            // fraction of world ranks in producer

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

    // ---  all ranks running workflow runtime system code ---

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * nblocks;

    // split the world into producer and consumer
    int producer_ranks = world.size() * prod_frac;
    bool producer = world.rank() < producer_ranks;
    diy::mpi::communicator local = world.split(producer);

    fmt::print("producer_ranks: {}\n", producer_ranks);

    MPI_Comm intercomm;
    MPI_Intercomm_create(local, 0, world, /* remote_leader = */ producer ? producer_ranks : 0, /* tag = */ 0, &intercomm);
    diy::mpi::communicator diy_intercomm(intercomm);

    fmt::print("local.size() = {}, intercomm.size() = {}\n", local.size(), diy_intercomm.size());

    // Set up file access property list with mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(plist, local, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(local, intercomm, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // --- ranks of producer task ---

    if (producer)
    {
        //  --- producer ranks running workflow runtime system code ---

        // set ownership of dataset (default is user (shallow copy), lowfive means deep copy)
        // filename and full path to dataset can contain '*' and '?' wild cards (ie, globs, not regexes)
        vol_plugin.data_ownership("outfile.h5", "/group1/*", l5::Dataset::Ownership::lowfive);

        // --- producer ranks running user task code  ---

        // diy setup for the producer
        diy::FileStorage                prod_storage(prefix);
        diy::Master                     prod_master(local,
                threads,
                mem_blocks,
                &Block::create,
                &Block::destroy,
                &prod_storage,
                &Block::save,
                &Block::load);
        AddBlock                        prod_create(prod_master);
        diy::ContiguousAssigner         prod_assigner(local.size(), nblocks);
        diy::RegularDecomposer<Bounds>  prod_decomposer(dim, domain, nblocks);
        prod_decomposer.decompose(local.rank(), prod_assigner, prod_create);

        // create a new file using default properties
        hid_t file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);
        hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        std::vector<hsize_t> domain_cnts(DIM);
        for (auto i = 0; i < DIM; i++)
            domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

        // create the file data space for the global grid
        hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

        // create the dataset with default properties
        hid_t dset = H5Dcreate2(group, "grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // write the data
        prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
                { b->write_block_hdf5(cp, dset); });

        // clean up
        H5Dclose(dset);
        H5Sclose(filespace);
        H5Gclose(group);
        H5Fclose(file);

        // signal the consumer that data are ready
        if (passthru && !metadata)
            diy_intercomm.barrier();

    }   // producer ranks

    // --- ranks of consumer task ---

    else
    {
        // wait for data to be ready
        if (passthru && !metadata)
            diy_intercomm.barrier();

        // create a new file using default properties
        hid_t file = H5Fopen("outfile.h5", H5F_ACC_RDONLY, plist);

        std::vector<hsize_t> domain_cnts(DIM);
        for (auto i = 0; i < DIM; i++)
            domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;

        // create the file data space for the global grid
        hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

        // open the dataset
        hid_t dset = H5Dopen(file, "/group1/grid", H5P_DEFAULT);

        // --- consumer ranks running user task code ---

        // diy setup for the consumer task on the consumer side
        diy::FileStorage                con_storage(prefix);
        diy::Master                     con_master(local,
                threads,
                mem_blocks,
                &Block::create,
                &Block::destroy,
                &con_storage,
                &Block::save,
                &Block::load);
        AddBlock                        con_create(con_master);
        diy::ContiguousAssigner         con_assigner(local.size(), con_nblocks);
        diy::RegularDecomposer<Bounds>  con_decomposer(dim, domain, con_nblocks);
        con_decomposer.decompose(local.rank(), con_assigner, con_create);

        // read the data
        con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
                { b->read_block_hdf5(cp, dset); });

        // clean up
        H5Dclose(dset);
        H5Sclose(filespace);
        H5Fclose(file);
    }       // consumer ranks

    // ---  all ranks running workflow runtime system code ---

    // clean up
    H5Pclose(plist);
}
