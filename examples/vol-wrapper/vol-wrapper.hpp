#pragma once

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
typedef     diy::DiscreteBounds         Bounds;
typedef     diy::RegularGridLink        Link;

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
    void generate_points(size_t n)                  // number of points
    {
        points.resize(n);
        for (size_t i = 0; i < n; ++i)
            for (unsigned j = 0; j < DIM; ++j)
                points[i][j] = domain.min[j] + float(rand() % 1024)/1024 *
                    (domain.max[j] - domain.min[j]);
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
            for (auto i = 0; i < DIM; i++)
                vertex[i] += bounds.min[i];         // converted to global domain vertex
            grid[i] = domain_grid.index(vertex);
        }
    }

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

    // write the block
    template<typename FileDriver>
    void write_(
            const diy::Master::ProxyWithLink&   cp,
            FileDriver&                         file_driver)
    {
        fmt::print("Writing in HighFive API...\n");

        // open file for parallel read/write
        l5::MetadataVOL vol_plugin;
        l5::H5VOLProperty vol_prop(vol_plugin);
        printf("our-vol-plugin registered: %d\n", H5VLis_connector_registered_by_name(vol_plugin.name.c_str()));
        file_driver.add(vol_prop);
        File file("outfile1.h5", File::ReadWrite | File::Create | File::Truncate, file_driver);

        // create top-level group
        Group group = file.createGroup("group1");

        // create and write the dataset for the points
        // 2d: dim[0] = number of ranks, dim[1] = number of point values (DIM per point)
        vector<size_t> pt_dims{ size_t(cp.master()->communicator().size()), points.size() * DIM };
        DataSet dataset = group.createDataSet<float>("points", DataSpace(pt_dims));
        vector<size_t> pt_ofst{ size_t(cp.master()->communicator().rank()), 0 };
        vector<size_t> pt_cnts{ 1, pt_dims[1] };
        dataset.select(pt_ofst, pt_cnts).write((float*)(&points[0]));

        // create and write the dataset for the bounds
        // 2d: dim[0] = number of ranks, dim[1] = number of bound values (DIM)
        vector<size_t> bound_dims{ size_t(cp.master()->communicator().size()), DIM };                          // local size
        DataSet dataset1 = group.createDataSet<float>("bounds_min", DataSpace(bound_dims));
        DataSet dataset2 = group.createDataSet<float>("bounds_max", DataSpace(bound_dims));
        vector<size_t> bound_ofst{ size_t(cp.master()->communicator().rank()), 0 };
        vector<size_t> bound_cnts{ 1, bound_dims[1] };
        dataset1.select(bound_ofst, bound_cnts).write((float*)(&bounds.min[0]));
        dataset2.select(bound_ofst, bound_cnts).write((float*)(&bounds.max[0]));

        // create the dataset for the grid
        vector<size_t>  grid_dims(DIM);             // global for entire domain
        for (auto i = 0; i < DIM; i++)
            grid_dims[i] = domain.max[i] - domain.min[i] + 1;
        DataSet dataset3 = group.createDataSet<float>("grid", DataSpace(grid_dims));

        // select all the grid points and write grid
        vector<size_t> grid_ofst(DIM), grid_cnts(DIM);
        for (auto i = 0; i < DIM; i++)
        {
            grid_ofst[i] = bounds.min[i];
            grid_cnts[i] = bounds.max[i] - bounds.min[i] + 1;
        }
        // TODO: following is hard-coded for DIM=3, hence the triple dereference
        // no way to tell HighFive to not check the dimensions of the data against the selection dims
        // NB: select in HighFive refers to file space, not memory space
        dataset3.select(grid_ofst, grid_cnts).write((float***)(&grid[0]));

        // debug: read back points and check that the written and read points match
        vector<Point> read_points(points.size());
        dataset.select(pt_ofst, pt_cnts).read((float*)(&read_points[0]));
        for (size_t i = 0; i < points.size(); ++i)
        {
            if (points[i] != read_points[i])
            {
                fmt::print("Error: points[{}] = {} but does not match read_points[{}] = {}\n", i, points[i], i, read_points[i]);
                exit(0);
            }
        }

        // debug: read back bounds and check that written and read bounds match
        Bounds read_bounds(DIM);
        dataset1.select(bound_ofst, bound_cnts).read((float*)(&read_bounds.min[0]));
        dataset2.select(bound_ofst, bound_cnts).read((float*)(&read_bounds.max[0]));
        for (auto i = 0; i < DIM; i++)
        {
            if (bounds.min[i] != read_bounds.min[i])
            {
                fmt::print("Error: bounds.min[{}] = {} but does not match read_bounds.min[{}] = {}\n", i, bounds.min[i], i, read_bounds.min[i]);
                exit(0);
            }
            if (bounds.max[i] != read_bounds.max[i])
            {
                fmt::print("Error: bounds.max[{}] = {} but does not match read_bounds.max[{}] = {}\n", i, bounds.max[i], i, read_bounds.max[i]);
                exit(0);
            }
        }

        // debug: read back grid
        vector<float> read_grid(grid.size());
        // TODO: following is hard-coded for DIM=3, hence the triple dereference
        // no way to tell HighFive to not check the dimensions of the data against the selection dims
        // NB: select in HighFive refers to file space, not memory space
        dataset3.select(grid_ofst, grid_cnts).read((float***)(&read_grid[0]));
        for (size_t i = 0; i < grid.size(); ++i)
        {
            if (grid[i] != read_grid[i])
            {
                fmt::print("Error: grid[{}] = {} but does not match read_grid[{}] = {}\n", i, grid[i], i, read_grid[i]);
                exit(0);
            }
        }

        vol_plugin.print_files();   // print out metadata before the file closes
        fmt::print("HighFive success.\n");
    }

    // write the block in parallel to an HDF5 file using HighFive API
    void write_block_highfive(
            const diy::Master::ProxyWithLink& cp,       // communication proxy
            bool                              core)     // whether to use core or MPI-IO file driver
    {
        if (core)
        {
            CoreFileDriver file_driver(1024);
            write_<CoreFileDriver>(cp, file_driver);
        }
        else
        {
            MPIOFileDriver file_driver((MPI_Comm)(cp.master()->communicator()), MPI_INFO_NULL);
            write_<MPIOFileDriver>(cp, file_driver);
        }
    }

    // block data
    Bounds              bounds      { DIM };        // local block bounds incl. ghost
    Bounds              core        { DIM };        // loacl block bounds excl. ghost
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
    void  operator()(int gid,                // block global id
                     const Bounds& core,     // block bounds without any ghost added
                     const Bounds& bounds,   // block bounds including any ghost region added
                     const Bounds& domain,   // global data bounds
                     const Link& link)       // neighborhood
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

