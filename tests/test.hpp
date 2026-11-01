#pragma once

#include <hdf5.h>
#include <mpi.h>
#include <lowfive/vol-base.hpp>
#include <lowfive/vol-metadata.hpp>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <utility>

#define CHECK(condition) do { if (!(condition)) throw std::runtime_error( \
    std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " #condition); } while (false)

namespace test
{
struct Id
{
    hid_t id = H5I_INVALID_HID;
    herr_t (*close)(hid_t) = nullptr;
    Id() = default;
    Id(hid_t value, herr_t (*closer)(hid_t)): id(value), close(closer) { CHECK(id >= 0); }
    ~Id() { if (id >= 0 && close) close(id); }
    Id(const Id&) = delete;
    Id& operator=(const Id&) = delete;
    Id(Id&& other) noexcept: id(std::exchange(other.id, H5I_INVALID_HID)), close(other.close) {}
    operator hid_t() const { return id; }
    void reset() { if (id >= 0 && close) CHECK(close(std::exchange(id, H5I_INVALID_HID)) >= 0); }
};

struct MPI
{
    MPI(int& argc, char**& argv) { CHECK(MPI_Init(&argc, &argv) == MPI_SUCCESS); }
    ~MPI() { MPI_Finalize(); }
};

// Each case runs in a separate process: LowFive has one active connector instance.
struct Connector
{
    LowFive::VOLBase& vol;
    hid_t connector;
    Id fapl;
    std::string filename;

    Connector(const std::string& name, const std::string& mode = "memory"):
        vol(mode == "native" ? LowFive::VOLBase::create_vol_base() :
                              LowFive::MetadataVOL::create_MetadataVOL()),
        connector(vol.register_plugin()), fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose),
        filename(name + ".h5")
    {
        if (mode != "native")
        {
            auto& metadata = dynamic_cast<LowFive::MetadataVOL&>(vol);
            if (mode != "file") metadata.set_memory(filename, "*");
            if (mode != "memory") metadata.set_passthru(filename, "*");
        }
        CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    }
    ~Connector()
    {
        fapl.reset();
        H5VLunregister_connector(connector);
        std::remove(filename.c_str());
    }
    Id file() { return Id(H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose); }
};

inline Id space(hsize_t size) { return Id(H5Screate_simple(1, &size, nullptr), H5Sclose); }
inline Id scalar() { return Id(H5Screate(H5S_SCALAR), H5Sclose); }

// A failing check must make CTest fail even in Release builds.
template<class Function>
int run(int argc, char** argv, Function function)
{
    try
    {
        MPI mpi(argc, argv);
        H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
        CHECK(argc == 2);
        function(std::string(argv[1]));
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::fprintf(stderr, "%s\n", error.what());
        return EXIT_FAILURE;
    }
}
}
