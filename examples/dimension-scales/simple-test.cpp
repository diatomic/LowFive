#include <iostream>

#include    <hdf5.h>
#include    <H5DSpublic.h>

#include <spdlog/spdlog.h>

herr_t fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
}


int main(int argc, char**argv)
{
    // get command line arguments

    // set HDF5 error handler
    H5Eset_auto(H5E_DEFAULT, fail_on_hdf5_error, NULL);

    // create the vol plugin

    // set up file access property list
    hid_t plist = H5Pcreate(H5P_FILE_ACCESS);

    std::string outfile = "out.h5";

   // create a new file and group using default properties
    hid_t file = H5Fcreate(outfile.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, plist);
    std::cerr << "H5Fcreate OK" << std::endl;

    H5Fclose(file);
    std::cerr << "H5Fclose OK" << std::endl;

    file = H5Fopen(outfile.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
    std::cerr << "H5Fopen OK, file = " << file << std::endl;

    hid_t group = H5Gcreate(file, "/group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    std::cerr << "H5Gcreate OK, group = " << group << std::endl;

    {
        hid_t file_1 = H5Fopen(outfile.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
        std::cerr << "H5Fopen 2nd call OK, file_1 = " << file_1 << std::endl;

        hid_t file_2 = H5Fopen(outfile.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
        std::cerr << "H5Fopen 2nd call OK, file_2 = " << file_2 << std::endl;

        hid_t group_1 = H5Gopen(file_1, "/group1", H5P_DEFAULT);
        std::cerr << "H5Gopen 2nd call OK, group_1 = " << group_1 << std::endl;


        hid_t group_as_obj = H5Oopen(file_1, "/group1", H5P_DEFAULT);
        std::cerr << "H5Oopen call OK, group_as_obj " << group_as_obj << std::endl;

        H5Gclose(group_1);
        std::cerr << "H5Gclose call OK, group_1 closed " << std::endl;

        H5Oclose(group_as_obj);
        std::cerr << "H5Oclose call OK, group_as_obj closed " << std::endl;

        H5Fclose(file_1);
        std::cerr << "H5Fclose call OK, file_1 closed " << std::endl;

        H5Fclose(file_2);
        std::cerr << "H5Fclose call OK, file_2 closed " << std::endl;
    }

    H5Gclose(group);
    std::cerr << "H5Gclose call OK, group closed " << std::endl;

    H5Fclose(file);
    std::cerr << "H5Fclose call OK, file closed " << std::endl;


//    // clean up
//    for (int i = 0; i < 1; i++)
//    {
//        H5Dclose(scale_dsets[i]);
//        H5Sclose(scale_spaces[i]);
//    }
//    H5Dclose(dset);
//    H5Sclose(filespace);
//    H5Gclose(group);
//    H5Fclose(file);
//    H5Pclose(plist);

    return 0;
}

