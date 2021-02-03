#include    <diy/master.hpp>
#include    <diy/reduce.hpp>
#include    <diy/partners/swap.hpp>
#include    <diy/decomposition.hpp>
#include    <diy/reduce-operations.hpp>
#include    <diy/assigner.hpp>
#include    <diy/../../examples/opts.h>

#include    <lowfive/metadata.hpp>

#include    "ghost-a2a.hpp"

static const unsigned DIM = 3;

using Block             = PointBlock<DIM>;
using AddBlock          = AddPointBlock<DIM>;
using Decomposer        = diy::RegularDecomposer<Bounds>;
using ObjectPointers    = l5::MetadataVOL::ObjectPointers;
using Dataset           = l5::Dataset;
using Dataspace         = l5::Dataspace;
using Datatype          = l5::Datatype;
using Object            = l5::Object;
using MetadataError     = l5::MetadataError;

void exchange_bounds(Block*                     b,
                     const diy::ReduceProxy&    rp,
                     const Decomposer&          decomposer)
{
    // send own bounds
    if (rp.in_link().size() == 0)
    {
        for (auto i = 0; i < rp.out_link().size(); ++i)         // include self
            rp.enqueue(rp.out_link().target(i), b->bounds);
    } else    // receive others' bounds
    {
        for (int i = 0; i < rp.in_link().size(); ++i)           // include self
        {
            int nbr_gid = rp.in_link().target(i).gid;
            Bounds in_bounds {DIM};
            rp.dequeue(nbr_gid, in_bounds);
//             fmt::print(stderr, "[{}] Received request for data in min {} max {} bounds from [{}]\n",
//                     rp.gid(), fmt::join(in_bounds.min, ","), fmt::join(in_bounds.max, ","), nbr_gid);
            b->query_bounds.push_back(std::make_pair(in_bounds, nbr_gid));
        }
    }
}

// assemble received data into one buffer
void
assemble_data(const std::vector<char>&  data_buf,       // received data buffers
              const std::vector<char>&  space_buf,      // received encoded dataspace buffers
              int                       dtype_size,     // element size of data in bytes
              char*                     assembled_data) // assembeld data
{
    // destination dataspace: decoded from received space_buf
    hid_t dst_id = H5Sdecode(space_buf.data());
    if (dst_id < 0)
        throw MetadataError(fmt::format("Error: decoded dataspace has invalid id\n"));
    Dataspace dst(dst_id, true);

    // iterate and copy
    size_t ofst = 0;
    Dataspace::iterate(dst, dtype_size, [&](size_t loc, size_t len)
    {
        std::memcpy(assembled_data + loc, data_buf.data() + ofst, len);
        ofst += len;
    });
}

// read any intersecting data stored locally in metadata
void
intersect_read(Dataset*                         dset,
               hid_t                            mem_space_id,
               hid_t                            file_space_id,
               std::vector<std::vector<char>>&  data_bufs,      // data buffers, one per intersection found
               std::vector<std::vector<char>>&  space_bufs)     // encoded dataspace buffers, one per intersection
{
    for (auto& dt : dset->data)                                 // for all the data triples in the metadata for the dataset
    {
        Dataspace& fs = dt.file;

        hid_t mem_dst = Dataspace::project_intersection(file_space_id, mem_space_id, fs.id);
        hid_t mem_src = Dataspace::project_intersection(fs.id,         dt.memory.id, file_space_id);

        Dataspace dst(mem_dst, true);
        Dataspace src(mem_src, true);

        if (!dst.size())
            continue;

        // for now assume that types and in particular type sizes must match
        if (dt.type.dtype_size != dset->type.dtype_size)
            throw MetadataError(fmt::format("Error: intersect_read(): mismatched datatype sizes {} vs. {}\n",
                        dt.type.dtype_size, dset->type.dtype_size));

        // copy data
        std::vector<char> data_buf(src.size() * dset->type.dtype_size);
        size_t ofst = 0;
        Dataspace::iterate(src, dset->type.dtype_size, [&](size_t loc, size_t len)
        {
            std::memcpy(data_buf.data() + ofst, (char*)dt.data + loc, len);
            ofst += len;
        });
        data_bufs.push_back(data_buf);

        // encode the destination space
        size_t nalloc = 0;
        H5Sencode(mem_dst, NULL, &nalloc, NULL);                // first time sets nalloc to the required size
        std::vector<char> space_buf(nalloc);
        H5Sencode(mem_dst, space_buf.data(), &nalloc, NULL);    // second time actually encodes the data
        space_bufs.push_back(space_buf);
    }
}

// fills data and space buffers for one queried set of bounds
void
fill_bufs(Bounds&                           bounds,
          Bounds&                           domain,
          Dataset*                          dset,
          std::vector<std::vector<char>>&   data_bufs,      // data buffers, one per intersection found
          std::vector<std::vector<char>>&   space_bufs)     // encoded dataspace buffers, one per intersection
{
    // number of grid points in bounds, domain
    std::vector<hsize_t> bounds_cnts(DIM);
    std::vector<hsize_t> domain_cnts(DIM);
    std::vector<hsize_t> ofst(DIM);
    for (auto j = 0; j < DIM; j++)
    {
        bounds_cnts[j]  = bounds.max[j] - bounds.min[j] + 1;
        domain_cnts[j]  = domain.max[j] - domain.min[j] + 1;
        ofst[j]         = bounds.min[j];
    }

    // filespace = bounds selected out of global domain
    hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);
    H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &bounds_cnts[0], NULL);

    // memspace = simple count from bounds
    hid_t memspace = H5Screate_simple (DIM, &bounds_cnts[0], NULL);

    // intersect with own data
    intersect_read(dset, memspace, filespace, data_bufs, space_bufs);

    H5Sclose(filespace);
    H5Sclose(memspace);
}

void exchange_data(Block*                   b,
                   const diy::ReduceProxy&  rp,
                   const Decomposer&        decomposer,
                   const l5::MetadataVOL&   mdata,              // VOL metadata
                   std::string              dset_full_path,     // full path to the dataset
                   std::string              filename,           // file containing the dataset
                   int                      dtype_size)         // datatype size in bytes
{
    if (rp.in_link().size() == 0)                               // send own data
    {
        Object* obj = mdata.locate(filename, dset_full_path);
        if (!obj)
            throw MetadataError(fmt::format("Error: exchange_data(): object not found in metadata"));
        if (obj->type != l5::ObjectType::Dataset)
            throw MetadataError(fmt::format("Error: exchange_data(): object found in metadata is not of type dataset"));
        Dataset* dset = static_cast<Dataset*>(obj);

        for (auto i = 0; i < b->query_bounds.size(); i++)
        {
            //         fmt::print("query_bounds[{}]: first: min {} max {} second: {}\n",
            //                 i, fmt::join(b->query_bounds[i].first.min, ","), fmt::join(b->query_bounds[i].first.max, ","), b->query_bounds[i].second);

            std::vector<std::vector<char>> data_bufs;           // data buffers, one per intersection found
            std::vector<std::vector<char>> space_bufs;          // encoded dataspace buffers, one per intersection

            fill_bufs(b->query_bounds[i].first, b->domain, dset, data_bufs, space_bufs);

            // send
            for (auto j = 0; j < rp.out_link().size(); ++j)
            {
                if (rp.out_link().target(j).gid == b->query_bounds[i].second)   // send only to requester
                {
//                     fmt::print(stderr, "[{}] Sending {} data buffers and {} dataspaces to [{}]\n",
//                             rp.gid(), data_bufs.size(), space_bufs.size(), b->query_bounds[i].second);
                    rp.enqueue(rp.out_link().target(j), data_bufs);
                    rp.enqueue(rp.out_link().target(j), space_bufs);
                }
            }
        }
    } else                                                      // receive others' data
    {
        for (auto j = 0; j < rp.in_link().size(); ++j)          // include self
        {
            std::vector<std::vector<char>> recv_data;           // incoming data buffers
            std::vector<std::vector<char>> recv_spaces;         // incoming dataspace buffers
            int nbr_gid = rp.in_link().target(j).gid;
            rp.dequeue(nbr_gid, recv_data);
            rp.dequeue(nbr_gid, recv_spaces);
//             fmt::print(stderr, "[{}] Received {} data buffers and {} dataspaces from [{}]\n",
//                     rp.gid(), recv_data.size(), recv_spaces.size(), nbr_gid);
            if (recv_data.size() != recv_spaces.size())
                throw MetadataError(fmt::format("Error: assemble_data(): size of data_bufs does not match size of space_bufs\n"));

            // assemble received data
            for (auto k = 0; k < recv_data.size(); k++)
                assemble_data(recv_data[k], recv_spaces[k], dtype_size, (char*)(b->recv_grid.data()));
        }
    }
}

int main(int argc, char* argv[])
{
    int   dim = DIM;

    diy::mpi::environment     env(argc, argv);
    diy::mpi::communicator    world;

    int                       nblocks     = world.size();   // global number of blocks
    int                       mem_blocks  = -1;             // all blocks in memory
    int                       threads     = 1;              // no multithreading
    int                       k           = 2;              // radix for k-ary reduction
    std::string               prefix      = "./DIY.XXXXXX"; // for saving block files out of core
    bool                      core        = false;          // in-core or MPI-IO file driver
    int                       ghost       = 0;              // number of ghost points (core - bounds) per side

    // default global data bounds
    Bounds domain { dim };
    domain.min[0] = domain.min[1] = domain.min[2] = 0;
    domain.max[0] = domain.max[1] = domain.max[2] = 100.;

    // get command line arguments
    using namespace opts;
    Options ops;
    ops
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
    Decomposer decomposer(dim, domain, nblocks, share_face, wrap, ghosts);
    decomposer.decompose(world.rank(), assigner, create);


    // set up file access property list with core or mpi-io file driver
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);
    if (core)
    {
        fmt::print("Using in-core file driver\n");
        H5Pset_fapl_core(plist, 1024 /* grow memory by this incremenet */, 0 /* bool backing_store (actual file) */);
    }
    else
    {
        fmt::print("Using mpi-io file driver\n");
        H5Pset_fapl_mpio(plist, master.communicator(), MPI_INFO_NULL);
    }

    // set up lowfive
    l5::MetadataVOL vol_plugin;
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

    // write the file
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->write_block_hdf5(cp, dset); });

    // exchange queried bounds
    diy::all_to_all(master,
                    assigner,
                    [&](Block* b, const diy::ReduceProxy& rp)
                        { exchange_bounds(b, rp, decomposer); },
                    2);

    // exchange queried data
    diy::all_to_all(master,
                    assigner,
                    [&](Block* b, const diy::ReduceProxy& rp)
                        { exchange_data(b,
                                        rp,
                                        decomposer,
                                        vol_plugin,
                                        std::string("/group1/grid"),
                                        std::string("outfile.h5"),
                                        sizeof(float)); },
                    2);

    // check if received data matches own data
    master.foreach([&](Block* b, const diy::Master::ProxyWithLink& cp)
            { b->check_recvd_data(cp); });

    // clean up
    herr_t status;
    status = H5Dclose(dset);
    status = H5Sclose(filespace);
    status = H5Gclose(group);
    status = H5Fclose(file);
    status = H5Pclose(plist);
}

