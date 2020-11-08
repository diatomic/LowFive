#pragma once

#include    <vector>
#include    <cassert>
#include    <diy/types.hpp>
#include    <diy/point.hpp>
#include    <diy/log.hpp>

#include    "hdf5.h"

#include    <highfive/H5DataSet.hpp>
#include    <highfive/H5DataSpace.hpp>
#include    <highfive/H5File.hpp>

#include    "vol-wrapper_vol.hpp"

using namespace HighFive;
using namespace std;

// D-dimensional point
template<unsigned D>
using SimplePoint = diy::Point<float, D>;

template <typename V>
struct VOLProperty
{
            VOLProperty(V& vol_plugin_):
                vol_plugin(vol_plugin_)
    {
        vol_id = vol_plugin.register_plugin();
    }
            ~VOLProperty()
    {
        H5VLterminate(vol_id);
        H5VLunregister_connector(vol_id);
        assert(H5VLis_connector_registered_by_name("our-vol-plugin") == 0);
    }

    void    apply(const hid_t list) const   { H5Pset_vol(list, vol_id, &vol_plugin.info); }

    hid_t   vol_id;
    V&      vol_plugin;
};

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
    typedef diy::ContinuousBounds                       Bounds;

    PointBlock(const Bounds& bounds_):
        bounds(bounds_)                 {}

    // allocate a new block
    static void* create()               { return new PointBlock; }
    // free a block
    static void destroy(void* b)        { delete static_cast<PointBlock*>(b); }
    // serialize the block and write it
    static void save(const void* b_, diy::BinaryBuffer& bb)
    {
        const PointBlock* b = static_cast<const PointBlock*>(b_);
        diy::save(bb, b->bounds);
        diy::save(bb, b->box);
        diy::save(bb, b->points);
    }
    // read the block and deserialize it
    static void  load(void* b_, diy::BinaryBuffer& bb)
    {
        PointBlock* b = static_cast<PointBlock*>(b_);
        diy::load(bb, b->bounds);
        diy::load(bb, b->box);
        diy::load(bb, b->points);
    }
    // initialize block values
    void generate_points(const Bounds& domain, // overall data bounds
                         size_t n)             // number of points
    {
        box = domain;
        points.resize(n);
        for (size_t i = 0; i < n; ++i)
            for (unsigned j = 0; j < DIM; ++j)
                points[i][j] = domain.min[j] + float(rand() % 1024)/1024 *
                    (domain.max[j] - domain.min[j]);
    }

    // check that block values are in the block bounds (debug)
    void verify_block(const diy::Master::ProxyWithLink& cp) // communication proxy
    {
        for (size_t i = 0; i < points.size(); ++i)
            for (unsigned j = 0; j < DIM; ++j)
                if (points[i][j] < box.min[j] || points[i][j] > box.max[j])
                {
                    fmt::print(stderr, "!!! Point outside the box !!!\n");
                    fmt::print(stderr, "    {}\n", points[i]);
                    fmt::print(stderr, "    {} - {}\n", box.min, box.max);
                }
    }
    // print block values
    void print_block(const diy::Master::ProxyWithLink& cp,  // communication proxy
                     bool verbose)                          // amount of output
    {
        fmt::print("[{}] Box:    {} -- {}\n", cp.gid(), box.min, box.max);
        fmt::print("[{}] Bounds: {} -- {}\n", cp.gid(), bounds.min, bounds.max);

        if (verbose)
        {
            for (size_t i = 0; i < points.size(); ++i)
                fmt::print("  {}\n", points[i]);
        } else
            fmt::print("[{}] Points: {}\n", cp.gid(), points.size());
    }

    // write the block in parallel to an HDF5 file using HighFive API
    void write_block_highfive(
            const diy::Master::ProxyWithLink& cp,       // communication proxy
            bool                              core)     // whether to use core or MPI-IO file driver
    {
        fmt::print("Writing in HighFive API...\n");

        // dataset size
        vector<size_t> dims(2);
        dims[0] = size_t(cp.master()->communicator().size());   // number of mpi ranks
        dims[1] = points.size() * DIM;                          // local size

        // debug: read-back data
        vector<Point> read_points(points.size());
        Bounds read_bounds(DIM);

        // open file for parallel read/write
        Vol vol_plugin { /* version = */ 0, /* value = */ 510, /* name = */ "our-vol-plugin" };
        VOLProperty<Vol> vol_prop(vol_plugin);
        printf("our-vol-plugin registered: %d\n", H5VLis_connector_registered_by_name("our-vol-plugin"));

        // write and read back
        if (core)               // in-core memory driver
        {
            fmt::print("Using in-core file driver\n");
            CoreFileDriver file_driver(1024);
            file_driver.add(vol_prop);
            File file("outfile1.h5", File::ReadWrite | File::Create | File::Truncate, file_driver);

            // create the dataset for the points
            DataSet dataset = file.createDataSet<float>("points", DataSpace(dims));

            // write points
            dataset.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&points[0]));

            // debug: read back points
            dataset.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_points[0]));

            // create the dataset for the bounds
            dims[1] = DIM;
            DataSet dataset1 = file.createDataSet<float>("bounds_min", DataSpace(dims));
            DataSet dataset2 = file.createDataSet<float>("bounds_max", DataSpace(dims));

            // write bounds
            dataset1.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&bounds.min[0]));
            dataset2.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&bounds.max[0]));

            // debug: read back bounds
            dataset1.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_bounds.min[0]));
            dataset2.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_bounds.max[0]));
        }                       // in-core memory
        else                    // file driver
        {
            fmt::print("Using mpi-io file driver\n");
            MPIOFileDriver file_driver((MPI_Comm)(cp.master()->communicator()), MPI_INFO_NULL);
            file_driver.add(vol_prop);
            File file("outfile1.h5", File::ReadWrite | File::Create | File::Truncate, file_driver);

            // create the dataset for the points
            DataSet dataset = file.createDataSet<float>("points", DataSpace(dims));

            // write points
            dataset.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&points[0]));

            // debug: read back points
            dataset.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_points[0]));

            // create the dataset for the bounds
            dims[1] = DIM;
            DataSet dataset1 = file.createDataSet<float>("bounds_min", DataSpace(dims));
            DataSet dataset2 = file.createDataSet<float>("bounds_max", DataSpace(dims));

            // write bounds
            dataset1.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&bounds.min[0]));
            dataset2.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                write((float*)(&bounds.max[0]));

            // debug: read back bounds
            dataset1.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_bounds.min[0]));
            dataset2.select({size_t(cp.master()->communicator().rank()), 0}, {1, dims[1]}).
                read((float*)(&read_bounds.max[0]));
        }                       // file driver

        // debug: check that the written and read points match
        for (size_t i = 0; i < points.size() / 2; ++i)
        {
            if (points[i] != read_points[i])
            {
                fmt::print("Error: points[{}] = {} but does not match read_points[{}] = {}\n", i, points[i], i, read_points[i]);
                exit(0);
            }
            //fmt::print("  {} == {}\n", points[i], read_points[i]);
        }

        // debug: check that written and read bounds match
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

        fmt::print("HighFive success.\n");
        vol_plugin.metadata->print_tree();
    }

    // block data
    Bounds              bounds { DIM };
    Bounds              box    { DIM };
    vector<Point>       points;

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
    typedef   diy::ContinuousBounds                               Bounds;
    typedef   diy::RegularContinuousLink                          RCLink;

    AddPointBlock(diy::Master& master_, size_t num_points_):
        master(master_),
        num_points(num_points_)
        {}

    // this is the function that is needed for diy::decompose
    void  operator()(int gid,                // block global id
                     const Bounds& core,     // block bounds without any ghost added
                     const Bounds& bounds,   // block bounds including any ghost region added
                     const Bounds& domain,   // global data bounds
                     const RCLink& link)     // neighborhood
        const
        {
            Block*          b   = new Block(core);
            RCLink*         l   = new RCLink(link);
            diy::Master&    m   = const_cast<diy::Master&>(master);

            m.add(gid, b, l); // add block to the master (mandatory)

            b->generate_points(domain, num_points); // initialize block data (typical)
        }

    diy::Master&  master;
    size_t        num_points;
};

