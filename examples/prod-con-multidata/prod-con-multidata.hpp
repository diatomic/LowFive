#pragma once

#include    <vector>
#include    <cassert>
#include    <diy/types.hpp>
#include    <diy/point.hpp>
#include    <diy/log.hpp>
#include    <diy/grid.hpp>
#include    <diy/reduce.hpp>
#include    <diy/partners/broadcast.hpp>

#include    "hdf5.h"

#include    <lowfive/H5VOLProperty.hpp>
#include    <lowfive/vol-dist-metadata.hpp>
#include    <lowfive/log.hpp>

namespace l5 = LowFive;

// D-dimensional point
template<unsigned D>
using SimplePoint = diy::Point<float, D>;

// using discrete bounds so that grid points are not duplicated on block boundaries
typedef     diy::DiscreteBounds                                 Bounds;
typedef     diy::RegularGridLink                                Link;
typedef     diy::RegularDecomposer<Bounds>::BoolVector          BoolVector;
typedef     diy::RegularDecomposer<Bounds>::CoordinateVector    CoordinateVector;

enum {producer_task, producer1_task, producer2_task, consumer_task, consumer1_task, consumer2_task};

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
        diy::save(bb, b->grid);
        diy::save(bb, b->box);
    }

    // read the block and deserialize it
    static void load(void* b_, diy::BinaryBuffer& bb)
    {
        PointBlock* b = static_cast<PointBlock*>(b_);
        diy::load(bb, b->bounds);
        diy::load(bb, b->domain);
        diy::load(bb, b->points);
        diy::load(bb, b->grid);
        diy::load(bb, b->box);
    }

    // initialize a regular grid of points in a block
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
            for (auto j = 0; j < DIM; j++)
                vertex[j] += bounds.min[j];         // converted to global domain vertex
            grid[i] = domain_grid.index(vertex);
        }

        // allocate second grid for received data to check agains original
        recv_grid.resize(grid.size());
    }

    // initialize a set of points in a block with values in a known sequence
    void generate_sequenced_points(int      gid,
                                   size_t   n)             // number of points
    {
        box = domain;
        points.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            for (unsigned j = 0; j < DIM; ++j)
                points[i][j] = gid * n + i;
//             fmt::print(stderr, "gid {} point [{}]\n", gid, points[i]);
        }
    }

    // initialize a set of points in a block
    void generate_points(const Bounds& domain, // overall data bounds
                         size_t n)             // number of points
    {
        box = domain;
        points.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            for (unsigned j = 0; j < DIM; ++j)
                points[i][j] = domain.min[j] + float(rand() % 1024)/1024 *
                    (domain.max[j] - domain.min[j]);
//             fmt::print(stderr, "Bounds [{} : {}] point [{}]\n", domain.min, domain.max, points[i]);
        }
    }

    // write the block grid data in parallel to an HDF5 file using native HDF5 API
    void write_block_grid(
            const diy::Master::ProxyWithLink& cp,           // communication proxy
            hid_t                             dset)
    {
//         fmt::print("write_block_grid(): writing gid {} bounds [min {} max {}]\n", cp.gid(), bounds.min, bounds.max);

        herr_t      status;

        // number of grid points in core, bounds, domain
        std::vector<hsize_t> core_cnts(DIM);
        std::vector<hsize_t> bounds_cnts(DIM);
        std::vector<hsize_t> domain_cnts(DIM);
        for (auto i = 0; i < DIM; i++)
        {
            core_cnts[i]    = core.max[i]   - core.min[i]   + 1;
            bounds_cnts[i]  = bounds.max[i] - bounds.min[i] + 1;
            domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;
        }

        // Create the file data space for the global grid
        hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);

        // filespace = core selected out of global domain
        std::vector<hsize_t> ofst(DIM);
        for (auto i = 0; i < DIM; i++)
            ofst[i] = core.min[i];
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        // memspace = core selected out of bounds
        hid_t memspace = H5Screate_simple (DIM, &bounds_cnts[0], NULL);
        for (auto i = 0; i < DIM; i++)
            ofst[i] = core.min[i] - bounds.min[i];
        status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        // write the dataset
        status = H5Dwrite(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &grid[0]);

        status = H5Sclose(filespace);
        status = H5Sclose(memspace);

        fmt::print("write_block_grid() gid {} success.\n", cp.gid());
    }

    // write the block particle data in parallel to an HDF5 file using native HDF5 API
    void write_block_points(
            const diy::Master::ProxyWithLink&   cp,
            hid_t                               dset,
            size_t                              global_num_blks)
    {
        herr_t      status;
        size_t      local_num_pts  = points.size();
        size_t      global_num_pts = global_num_blks * local_num_pts;       // same number of local points per block

        // number of particles in core and domain
        std::vector<hsize_t> core_cnts(2);      // dim 0: index of point ; dim 1: coordinates of point
        std::vector<hsize_t> domain_cnts(2);
        core_cnts[0]    = local_num_pts;
        core_cnts[1]    = DIM;
        domain_cnts[0]  = global_num_pts;
        domain_cnts[1]  = DIM;

        // filespace = core selected out of global domain
        hid_t filespace = H5Screate_simple(2, &domain_cnts[0], NULL);
        std::vector<hsize_t> ofst(2);
        ofst[0] = cp.gid() * local_num_pts;                 // assumes each block has same number of particles
        ofst[1] = 0;
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        fmt::print("write_block_particles(): writing gid {} bounds [min {} max {}] core_cnts {} domain_cts {} ofst {}\n",
                cp.gid(), bounds.min, bounds.max, core_cnts[0], domain_cnts[0], ofst[0]);

        // memspace = simple local number of particles
        hid_t memspace = H5Screate_simple (2, &core_cnts[0], NULL);

        // write the dataset
        status = H5Dwrite(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &points[0]);

        status = H5Sclose(filespace);
        status = H5Sclose(memspace);

        fmt::print("write_block_points() gid {} success.\n", cp.gid());
    }

    // read the block grid data in parallel to an HDF5 file using native HDF5 API
    void read_block_grid(
            const diy::Master::ProxyWithLink& cp,           // communication proxy
            hid_t                             dset)
    {
        herr_t      status;

//         fmt::print("reading gid {} bounds [min {} max {}]\n", cp.gid(), bounds.min, bounds.max);

        // read back the grid as a test
        std::vector<float> read_grid(grid.size());

        // number of grid points in core, bounds, domain
        std::vector<hsize_t> bounds_cnts(DIM);
        std::vector<hsize_t> domain_cnts(DIM);
        std::vector<hsize_t> ofst(DIM);
        for (auto i = 0; i < DIM; i++)
        {
            bounds_cnts[i]  = bounds.max[i] - bounds.min[i] + 1;
            domain_cnts[i]  = domain.max[i] - domain.min[i] + 1;
            ofst[i] = bounds.min[i];
        }

        // filespace = bounds selected out of global domain
        hid_t filespace = H5Screate_simple(DIM, &domain_cnts[0], NULL);
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &bounds_cnts[0], NULL);

        // memspace = simple count from bounds
        hid_t memspace = H5Screate_simple (DIM, &bounds_cnts[0], NULL);
        status = H5Dread(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &read_grid[0]);

        // check that the values match
        bool success = true;
        for (size_t i = 0; i < grid.size(); ++i)
        {
            if (grid[i] != read_grid[i])
            {
                success = false;
                fmt::print("Error: grid[{}] = {} but does not match read_grid[{}] = {}\n", i, grid[i], i, read_grid[i]);
//                 exit(0);
            }
        }

        status = H5Sclose(filespace);
        status = H5Sclose(memspace);

        if (success)
            fmt::print("read_block_grid() gid {} success.\n", cp.gid());
    }

    // read the block particle data in parallel to an HDF5 file using native HDF5 API
    void read_block_points(
            const diy::Master::ProxyWithLink&   cp,
            hid_t                               dset,
            size_t                              global_num_pts,
            size_t                              global_num_blks)
    {
        herr_t              status;
        size_t              local_num_pts = points.size();
        std::vector<Point>  read_points(local_num_pts);         // points being read back

        // number of particles in core, domain
        std::vector<hsize_t> core_cnts(2);      // dim 0: index of point ; dim 1: coordinates of point
        std::vector<hsize_t> domain_cnts(2);
        core_cnts[0]    = local_num_pts;
        core_cnts[1]    = DIM;
        domain_cnts[0]  = global_num_pts;
        domain_cnts[1]  = DIM;

        // filespace = core selected out of global domain
        std::vector<hsize_t> ofst(2);
        ofst[0] = cp.gid() * (global_num_pts / global_num_blks);
        ofst[1] = 0;
        hid_t filespace = H5Screate_simple(2, &domain_cnts[0], NULL);
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &ofst[0], NULL, &core_cnts[0], NULL);

        // memspace = simple local number of particles
        hid_t memspace = H5Screate_simple (2, &core_cnts[0], NULL);
        status = H5Dread(dset, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, &read_points[0]);

        // check that the values match
        bool success = true;
        for (size_t i = 0; i < read_points.size(); ++i)
        {
            for (auto j = 0; j < DIM; j++)
            {
                float min_val = cp.gid() * (global_num_pts / global_num_blks);
                float max_val = min_val + read_points.size() - 1;
                if (read_points[i][j] < min_val || read_points[i][j] > max_val)
                {
                    fmt::print(stderr, "Error: for gid {}, read_points[{}] = {} < {} or > {}\n", cp.gid(), i, read_points[i], min_val, max_val);
                    success = false;
//                     exit(0);
                }
            }
        }

        status = H5Sclose(filespace);
        status = H5Sclose(memspace);

        if (success)
            fmt::print("read_block_points() gid {} success.\n", cp.gid());
    }

    // block data
    Bounds              bounds      { DIM };        // local block bounds incl. ghost
    Bounds              core        { DIM };        // local block bounds excl. ghost
    Bounds              domain      { DIM };        // global domain bounds
    Bounds              box         { DIM };        // temporary box for whatever purpose
    std::vector<Point>  points;                     // unstructured set of points
    std::vector<float>  grid;                       // scalars linearized from a structured grid
    std::vector<float>  recv_grid;                  // data received from others
    std::vector<std::pair<Bounds, int>> query_bounds;   // bounds requested by other blocks

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

    AddPointBlock(diy::Master&  master_,
                  size_t        local_num_points_,
                  size_t        global_num_points_,
                  size_t        global_num_blocks_):
        master(master_),
        local_num_points(local_num_points_),
        global_num_points(global_num_points_),
        global_num_blocks(global_num_blocks_)
        {}

    // this is the function that is needed for diy::decompose
    void  operator()(int            gid,            // block global id
                     const  Bounds& core,           // block bounds without any ghost added
                     const  Bounds& bounds,         // block bounds including any ghost region added
                     const  Bounds& domain,         // global data bounds
                     const  Link&   link)           // neighborhood
        {
            Block*          b   = new Block(core, bounds, domain);
            Link*           l   = new Link(link);
            diy::Master&    m   = const_cast<diy::Master&>(master);

            m.add(gid, b, l);

            // NB, only the producer really needs to generate the grid and the points because the
            // consumer gets them from the workflow, but we are re-using the same AddPointBlock
            // function for both producer and consumer in this example

            b->generate_grid();                     // initialize block with regular grid

            // adjust local number of points if global number of points does not divide global number of blocks evenly
            if (local_num_points * global_num_blocks < global_num_points && gid == global_num_blocks - 1)
                local_num_points = global_num_points - local_num_points * (global_num_blocks - 1);

            b->generate_sequenced_points(gid, local_num_points); // initialize block with set of points
        }

    diy::Master&    master;
    size_t          local_num_points;
    size_t          global_num_points;
    size_t          global_num_blocks;
};

static const unsigned DIM = 3;
typedef     PointBlock<DIM>             Block;
typedef     AddPointBlock<DIM>          AddBlock;
