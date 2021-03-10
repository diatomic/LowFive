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

    // ---  all ranks running workflow runtime system code ---

    // consumer will read different block decomposition than the producer
    // producer also needs to know this number so it can match collective operations
    int con_nblocks = pow(2, dim) * nblocks;

    // split the world into producer (first half of ranks) and consumer (remainder)
    diy::mpi::communicator prod_comm    = world.split(world.rank() < world.size() / 2);
    diy::mpi::communicator con_comm     = world.split(world.rank() >= world.size() / 2);

    // Set up file access property list with mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(plist, world, MPI_INFO_NULL);

    // set up lowfive
    l5::DistMetadataVOL vol_plugin(world, metadata, passthru);
    l5::H5VOLProperty vol_prop(vol_plugin);
    vol_prop.apply(plist);

    // --- ranks of producer task ---

    if (world.rank() < prod_comm.size())
    {
        // --- producer ranks running user task code  ---

        // diy setup for the producer
        diy::FileStorage                prod_storage(prefix);
        diy::Master                     prod_master(prod_comm,
                threads,
                mem_blocks,
                &Block::create,
                &Block::destroy,
                &prod_storage,
                &Block::save,
                &Block::load);
        AddBlock                        prod_create(prod_master);
        diy::ContiguousAssigner         prod_assigner(prod_comm.size(), nblocks);
        diy::RegularDecomposer<Bounds>  prod_decomposer(dim, domain, nblocks);
        prod_decomposer.decompose(prod_comm.rank(), prod_assigner, prod_create);

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
        prod_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
                { b->write_block_hdf5(cp, dset); });

        // --- producer ranks running workflow system code  ---

        // null read (needed to participate in the collective querying of the consumer)
        // TODO: assume consumer told workflow system how many blocks per rank it had
        // TODO: indexing does not work if some rank has a different number of blocks (remainder)
        // TODO: this is a total hack
        int con_bpr = con_nblocks / con_comm.size();
        for (auto i = 0; i < con_bpr; i++)
            read_null(dset, DIM);

        // clean up
        H5Dclose(dset);
        H5Sclose(filespace);
        H5Gclose(group);
        H5Fclose(file);
    }       // producer ranks

    // --- ranks of consumer task ---

    else
    {
        // --- consumer ranks running workflow system code ---

        // assume the producer broadcasted the following to the workflow system

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

        // null write (needed to participate in the collective indexing of the producer)
        // TODO: assume producer told workflow system how many blocks per rank it had
        // TODO: indexing does not work if some rank has a different number of blocks (remainder)
        // TODO: this is a total hack
        int prod_bpr = nblocks / prod_comm.size();
        for (auto i = 0; i < prod_bpr; i++)
            write_null(dset, DIM);

        // --- consumer ranks running user task code ---

        // diy setup for the consumer task on the consumer side
        diy::FileStorage                con_storage(prefix);
        diy::Master                     con_master(con_comm,
                threads,
                mem_blocks,
                &Block::create,
                &Block::destroy,
                &con_storage,
                &Block::save,
                &Block::load);
        AddBlock                        con_create(con_master);
        diy::ContiguousAssigner         con_assigner(con_comm.size(), con_nblocks);
        diy::RegularDecomposer<Bounds>  con_decomposer(dim, domain, con_nblocks);
        con_decomposer.decompose(con_comm.rank(), con_assigner, con_create);

        // read the data
        con_master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
                { b->read_block_hdf5(cp, dset); });

        // clean up
        H5Dclose(dset);
        H5Sclose(filespace);
        H5Gclose(group);
        H5Fclose(file);
    }       // consumer ranks

    // ---  all ranks running workflow runtime system code ---

    // clean up
    H5Pclose(plist);
}
