#include    "hdf5.h"

#include    <highfive/H5DataSet.hpp>
#include    <highfive/H5DataSpace.hpp>
#include    <highfive/H5File.hpp>
#include    <highfive/H5FileDriver.hpp>
namespace h5 = HighFive;

#include    <lowfive/H5VOLProperty.hpp>
#include    <lowfive/vol-metadata.hpp>
namespace l5 = LowFive;

#include    <diy/../../examples/opts.h>
#include    <diy/mpi.hpp>

int main(int argc, char* argv[])
{
    // we don't actually use MPI here now, but just in case
    diy::mpi::environment     env(argc, argv); // equivalent of MPI_Init(argc, argv)/MPI_Finalize()
    diy::mpi::communicator    world;           // equivalent of MPI_COMM_WORLD

    // get command line arguments
    using namespace opts;
    Options ops;

    bool vol, base, help;
    ops
        >> Option('v', "vol",     vol,      "use VOL")
        >> Option(     "base",    base,     "use VOLBase instead of MetadataVOL (must be used with -v)")
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

    // open file for parallel read/write
    l5::MetadataVOL metadata_vol_plugin;
    l5::VOLBase     base_vol_plugin(0, 511, "vol-base");
    printf("Vol plugin registered: %d\n", H5VLis_connector_registered_by_name(metadata_vol_plugin.name.c_str()));
    printf("Vol plugin registered: %d\n", H5VLis_connector_registered_by_name(base_vol_plugin.name.c_str()));

    h5::FileDriver file_driver;
    if (vol)
    {
        printf("Adding VOL\n");
        if (base)
            file_driver.add(l5::H5VOLProperty(base_vol_plugin));
        else
            file_driver.add(l5::H5VOLProperty(metadata_vol_plugin));
    }
    h5::File file("variety1.h5", h5::File::ReadWrite | h5::File::Create | h5::File::Truncate, file_driver);

    h5::Group group = file.createGroup("group1");

    /* write a dataset */
    std::vector<size_t> dims{2, 6};

    // Create the dataset
    h5::DataSet dataset =
        group.createDataSet<double>("values1", h5::DataSpace(dims));

    double data[2][6] = {{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
                         {11.11, 12.12, 13.13, 14.14, 15.15, 16.16}};

    // write it
    dataset.write(data);


    /* write an attribute */
    std::vector<int> version { 1, 0 };
    h5::Attribute v = group.createAttribute<int>("version", h5::DataSpace::From(version));
    v.write(version);


    /* create a compound datatype */
    int dim = 3;
    hid_t comp_dtype = H5Tcreate (H5T_COMPOUND, 2 * dim * sizeof(int));
    H5Tinsert (comp_dtype, "lo_i", 0 * sizeof(int), H5T_NATIVE_INT);
    H5Tinsert (comp_dtype, "lo_j", 1 * sizeof(int), H5T_NATIVE_INT);
    H5Tinsert (comp_dtype, "lo_k", 2 * sizeof(int), H5T_NATIVE_INT);
    H5Tinsert (comp_dtype, "hi_i", 3 * sizeof(int), H5T_NATIVE_INT);
    H5Tinsert (comp_dtype, "hi_j", 4 * sizeof(int), H5T_NATIVE_INT);
    H5Tinsert (comp_dtype, "hi_k", 5 * sizeof(int), H5T_NATIVE_INT);
    
    std::vector<int> domain { 2, 2, 2, 6, 6, 6 };
    hid_t aid = H5Screate(H5S_SCALAR);
    hid_t domain_attr = H5Acreate(group.getId(), "prob_domain", comp_dtype, aid, H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(domain_attr, comp_dtype, domain.data());
    H5Aclose(domain_attr);
    H5Sclose(aid);
    
    H5Tclose(comp_dtype);


    metadata_vol_plugin.print_files();   // print out metadata before the file closes

    auto* f = metadata_vol_plugin.files["variety1.h5"];
    if (f)
        LowFive::save(*f, "lowfive-variety1.h5");
}
