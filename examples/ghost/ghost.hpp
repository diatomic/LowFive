#pragma once

#include    <util.hpp>

#include    <vector>
#include    <cassert>
#include    <diy/types.hpp>
#include    <diy/point.hpp>
#include    <diy/log.hpp>
#include    <diy/grid.hpp>

#include    "hdf5.h"

#include    <highfive/H5DataSet.hpp>
#include    <highfive/H5DataSpace.hpp>
#include    <highfive/H5File.hpp>
#include    <core_file_driver.hpp>

#include    <lowfive/H5VOLProperty.hpp>
#include    <lowfive/vol-metadata.hpp>
namespace l5 = LowFive;

using namespace HighFive;
using namespace std;

// D-dimensional point
template<unsigned D>
using SimplePoint = diy::Point<float, D>;

// using discrete bounds so that grid points are not duplicated on block boundaries
typedef     diy::DiscreteBounds                                 Bounds;
typedef     diy::RegularGridLink                                Link;
typedef     diy::RegularDecomposer<Bounds>::BoolVector          BoolVector;
typedef     diy::RegularDecomposer<Bounds>::CoordinateVector    CoordinateVector;

// block structure
// the contents of a block are completely user-defined
// however, a block must have functions defined to create, destroy, save, and load it
// create and destroy allocate and free the block, while
// save and load serialize and deserialize the block
// these four functions are called when blocks are cycled in- and out-of-core
// they can be member functions of the block, as below, or separate standalone functions
template<unsigned DIM>
struct PointBlock
{
    typedef SimplePoint<DIM>                            Point;

    PointBlock(const Bounds& core_, const Bounds& bounds_, const Bounds& domain_):
        core(core_),
        bounds(bounds_),
        domain(domain_)                 {}

    // allocate a new block
    static void* create()               { return new PointBlock; }
    // free a block
    static void destroy(void* b)        { delete static_cast<PointBlock*>(b); }
    // serialize the block and write it
    static void save(const void* b_, diy::BinaryBuffer& bb)
    {
        const PointBlock* b = static_cast<const PointBlock*>(b_);
        diy::save(bb, b->bounds);
        diy::save(bb, b->domain);
        diy::save(bb, b->points);
    }
    // read the block and deserialize it
    static void  load(void* b_, diy::BinaryBuffer& bb)
    {
        PointBlock* b = static_cast<PointBlock*>(b_);
        diy::load(bb, b->bounds);
        diy::load(bb, b->domain);
        diy::load(bb, b->points);
    }

    // initialize an unstructured set of points in a block
    void generate_points(const size_t n)              // number of points
    {
        points.resize(n);
        for (size_t i = 0; i < n; ++i)
            for (unsigned j = 0; j < DIM; ++j)
                points[i][j] = domain.min[j] + float(rand() % 1024)/1024 *
                    (domain.max[j] - domain.min[j]);
    }

#if 1

    // initialize a regular grid of points in a block
    // using DIY's Grid class
    void generate_grid()                            // local block bounds
    {
        using GridPoint = diy::Point<size_t, DIM>;
        using Grid = diy::GridRef<float, DIM>;
        GridPoint shape, vertex;

        // virtual grid covering local block bounds (for indexing only, no associated data)
        for (auto i = 0; i < DIM; i++)
            shape[i] = bounds.max[i] - bounds.min[i] + 1;
        Grid bounds_grid(NULL, shape);

        // virtual grid covering global domain (for indexing only, no associated data)
        for (auto i = 0; i < DIM; i++)
            shape[i] = domain.max[i] - domain.min[i] + 1;
        Grid domain_grid(NULL, shape);

        // assign globally unique values to the grid scalars in the block
        // equal to global linear idx of the grid point
        grid.resize(bounds_grid.size());
        for (auto i = 0; i < bounds_grid.size(); i++)
        {
            vertex = bounds_grid.vertex(i);         // vertex in the local block
            for (auto i = 0; i < DIM; i++)
                vertex[i] += bounds.min[i];         // converted to global domain vertex
            grid[i] = domain_grid.index(vertex);
        }
    }

#else

    // initialize a regular grid of points in a block
    // using Tom's VolIterator
    void generate_grid()                            // local block bounds
    {
        // set up volume iterator for the block grid
        vector<size_t> block_npts(DIM), block_starts(DIM), dom_npts(DIM);
        size_t grid_npts = 1;                       // total number of grid points in the block
        for (auto i = 0; i < DIM; i++)
        {
            // swap dimensions to be C order to match DIY bounds and HDF5 dataspaces ordering
            block_npts[DIM - i - 1]   = bounds.max[i] - bounds.min[i] + 1;
            block_starts[DIM - i - 1] = bounds.min[i];
            dom_npts[DIM - i - 1]     = domain.max[i] - domain.min[i] + 1;
            grid_npts                 *= block_npts[DIM - i - 1];
        }
        VolIterator vi(block_npts, block_starts, dom_npts);

        // assign globally unique values to the grid scalars, equal to global linear idx of the grid point
        grid.resize(grid_npts);
        while (!vi.done())
        {
            grid[vi.cur_iter()] = vi.sub_full_idx(vi.cur_iter());   // value = index in global domain

            // debug: print a few values
//             if (vi.cur_iter() < 10)
//                 fmt::print(stderr, "writing {}\n", grid[vi.cur_iter()]);

            vi.incr_iter();
        }
    }

#endif

    // check that block values are in the block bounds (debug)
    void verify_block(const diy::Master::ProxyWithLink& cp) // communication proxy
    {
        for (size_t i = 0; i < points.size(); ++i)
            for (unsigned j = 0; j < DIM; ++j)
                if (points[i][j] < domain.min[j] || points[i][j] > domain.max[j])
                {
                    fmt::print(stderr, "!!! Point outside the domain !!!\n");
                    fmt::print(stderr, "    {}\n", points[i]);
                    fmt::print(stderr, "    {} - {}\n", domain.min, domain.max);
                }
    }
    // print block values
    void print_block(const diy::Master::ProxyWithLink& cp,  // communication proxy
                     bool verbose)                          // amount of output
    {
        fmt::print("[{}] Domain: {} -- {}\n", cp.gid(), domain.min, domain.max);
        fmt::print("[{}] Bounds: {} -- {}\n", cp.gid(), bounds.min, bounds.max);

        if (verbose)
        {
            for (size_t i = 0; i < points.size(); ++i)
                fmt::print("  {}\n", points[i]);
        } else
            fmt::print("[{}] Points: {}\n", cp.gid(), points.size());
    }

    // write the block in parallel to an HDF5 file using native HDF5 API
    void write_block_hdf5(
            const diy::Master::ProxyWithLink& cp,           // communication proxy
            bool                              core_driver)  // whether to use core or MPI-IO file driver
    {
        fmt::print("Writing in native HDF5 API...\n");

        hid_t       file, dset, memspace, filespace, plist, group;      // identifiers
        herr_t      status;

        // Set up file access property list with core or mpi-io file driver
        plist = H5Pcreate(H5P_FILE_ACCESS);
        if (core_driver)
        {
            fmt::print("Using in-core file driver\n");
            H5Pset_fapl_core(plist, 1024 /* grow memory by this incremenet */, 0 /* bool backing_store (actual file) */);
        }
        else
        {
            fmt::print("Using mpi-io file driver\n");
            H5Pset_fapl_mpio(plist, cp.master()->communicator(), MPI_INFO_NULL);
        }

        // set up lowfive
        l5::MetadataVOL vol_plugin;
        l5::H5VOLProperty vol_prop(vol_plugin);
        vol_prop.apply(plist);

        // Create a new file using default properties
        file = H5Fcreate("outfile.h5", H5F_ACC_TRUNC, H5P_DEFAULT, plist);

        // Create top-level group
        group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // number of grid points in core, bounds, domain
        vector<hsize_t> core_cnts(DIM);
        vector<hsize_t> bounds_cnts(DIM);
        vector<hsize_t> domain_cnts(DIM);
        for (auto i = 0; i < DIM; i++)
        {
            core_cnts[i]    = core.max[i]   - core.min[i]   + 1;
            bounds_cnts[i]  = bounds.max[i] - bounds.min[i] + 1;
            domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;
        }

        // Create the file data space for the global grid
        filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

        // Create the dataset
        dset = H5Dcreate2(group, "/group1/grid", H5T_IEEE_F32LE, filespace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

        // filespace = core selected out of global domain
        vector<hsize_t> ofst(DIM);
        for (auto i = 0; i < DIM; i++)
            ofst[i] = core.min[i];
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        // memspace = core selected out of bounds
        memspace = H5Screate_simple (DIM, &bounds_cnts[0], NULL);
        for (auto i = 0; i < DIM; i++)
            ofst[i] = core.min[i] - bounds.min[i];
        status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        // write the dataset
        status = H5Dwrite(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &grid[0]);

        // read back the grid as a test
        // include the ghost in the read back, so that the read grid matches what is in the block bounds
        vector<float> read_grid(grid.size());

        // filespace = bounds selected out of global domain
        status = H5Sclose(filespace);
        filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);
        for (auto i = 0; i < DIM; i++)
            ofst[i] = bounds.min[i];
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &bounds_cnts[0], NULL);

        // memspace = simple count from bounds
        status = H5Sclose(memspace);
        memspace = H5Screate_simple (DIM, &bounds_cnts[0], NULL);
        status = H5Dread(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &read_grid[0]);

        // check that the values match
        for (size_t i = 0; i < grid.size(); ++i)
        {
            if (grid[i] != read_grid[i])
            {
                fmt::print("Error: grid[{}] = {} but does not match read_grid[{}] = {}\n", i, grid[i], i, read_grid[i]);
                exit(0);
            }
        }

        // clean up
        status = H5Dclose(dset);
        status = H5Sclose(memspace);
        status = H5Sclose(filespace);
        status = H5Pclose(plist);
        status = H5Gclose(group);
        status = H5Fclose(file);

        fmt::print("HDF5 success.\n");
    }

    // block data
    Bounds              bounds      { DIM };        // local block bounds incl. ghost
    Bounds              core        { DIM };        // local block bounds excl. ghost
    Bounds              domain      { DIM };        // global domain bounds
    vector<Point>       points;                     // unstructured set of points
    vector<float>       grid;                       // scalars linearized from a structured grid

private:
    PointBlock()                                  {}
};

// diy::decompose needs to have a function defined to create a block
// here, it is wrapped in an object to add blocks with an overloaded () operator
// it could have also been written as a standalone function
template<unsigned DIM>
struct AddPointBlock
{
    typedef   PointBlock<DIM>                                     Block;

    AddPointBlock(diy::Master& master_, size_t num_points_):
        master(master_),
        num_points(num_points_)
        {}

    // this is the function that is needed for diy::decompose
    void  operator()(int            gid,            // block global id
                     const  Bounds& core,           // block bounds without any ghost added
                     const  Bounds& bounds,         // block bounds including any ghost region added
                     const  Bounds& domain,         // global data bounds
                     const  Link&   link)           // neighborhood
        const
        {
            Block*          b   = new Block(core, bounds, domain);
            Link*           l   = new Link(link);
            diy::Master&    m   = const_cast<diy::Master&>(master);

            m.add(gid, b, l); // add block to the master (mandatory)

            // initialize the block with a set of points and a regular grid (e.g.)
            b->generate_points(num_points);
            b->generate_grid();
        }

    diy::Master&  master;
    size_t        num_points;
};

