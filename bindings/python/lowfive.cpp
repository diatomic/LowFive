#include <vector>
#include <type_traits>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
namespace py = pybind11;

#if defined(LOWFIVE_MPI4PY)
#include "mpi_comm.h"
#endif

#include <lowfive/vol-metadata.hpp>
#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/../../src/log-private.hpp>

// MPI_Comm is a pointer in OpenMPI
template<class Comm, typename std::enable_if<std::is_pointer<Comm>::value,bool>::type = true>
py::capsule to_capsule(Comm comm)
{
    return py::capsule(comm);
}

template<class Comm>
typename std::enable_if<std::is_pointer<Comm>::value, Comm>::type
from_capsule(py::capsule c)
{
    return c;
}

// MPI_Comm is an integer in MPICH
template<class Comm, typename std::enable_if<std::is_integral<Comm>::value,bool>::type = true>
py::capsule to_capsule(Comm comm)
{
    intptr_t comm_ = static_cast<intptr_t>(comm);
    void* comm__ = reinterpret_cast<void*>(comm_);
    return py::capsule(comm__);
}

template<class Comm>
typename std::enable_if<std::is_integral<Comm>::value, Comm>::type
from_capsule(py::capsule c)
{
    void* comm_ = c;
    intptr_t comm__ = reinterpret_cast<intptr_t>(comm_);
    Comm comm = static_cast<Comm>(comm__);
    return comm;
}


PYBIND11_MODULE(_lowfive, m)
{
    using namespace pybind11::literals;

    m.doc() = "LowFive python bindings";

#if defined(LOWFIVE_MPI4PY)
    // import the mpi4py API
    if (import_mpi4py() < 0)
        throw std::runtime_error("Could not load mpi4py API.");
#endif

    m.def("create_logger", [](std::string lev) { LowFive::create_logger(lev); return 0; }, "Create spdlog logger for LowFive");

    py::class_<LowFive::VOLBase> vol_base(m, "VOLBase", "base VOL object");
    vol_base
        .def(py::init<>(), "construct the object")
    ;

    py::class_<LowFive::MetadataVOL> metadata_vol(m, "MetadataVOL", "metadata VOL object", vol_base);
    metadata_vol
        .def(py::init<>(), "construct the object")
        .def("set_passthru",   &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",     &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy",   &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
        .def("set_keep",       &LowFive::MetadataVOL::set_keep,     "keep_a",                  "set whether to keep files in the metadata after they are closed")
        .def("print_files",    &LowFive::MetadataVOL::print_files,                             "print file metadata")
    ;

    py::class_<LowFive::DistMetadataVOL> dist_metadata_vol(m, "DistMetadataVOL", "metadata VOL object", metadata_vol);
    dist_metadata_vol
        .def(py::init([](py::capsule local, py::capsule intercomm)
                      {
                        return new LowFive::DistMetadataVOL(from_capsule<MPI_Comm>(local), from_capsule<MPI_Comm>(intercomm));
                      }),  "local"_a, "intercomm"_a,  "construct the object")
        .def(py::init([](py::capsule local, std::vector<py::capsule> intercomms)
                      {
                          MPI_Comm local_ = from_capsule<MPI_Comm>(local);
                          std::vector<MPI_Comm> intercomms_;
                          for (auto& c : intercomms)
                            intercomms_.push_back(from_capsule<MPI_Comm>(c));
                          return new LowFive::DistMetadataVOL(local_, intercomms_);
                      }), "local"_a, "intercomms"_a, "construct the object")
#if defined(LOWFIVE_MPI4PY)
        .def(py::init<mpi4py_comm, mpi4py_comm>(),  "local"_a, "intercomm"_a,  "construct the object")
        .def(py::init([](mpi4py_comm local, std::vector<mpi4py_comm> intercomms)
                      {
                          MPI_Comm local_ = local;
                          std::vector<MPI_Comm> intercomms_(intercomms.begin(), intercomms.end());
                          return new LowFive::DistMetadataVOL(local_, intercomms_);
                      }), "local"_a, "intercomms"_a, "construct the object")
#endif
        .def_readwrite("serve_on_close",   &LowFive::DistMetadataVOL::serve_on_close)
        .def("set_intercomm",   &LowFive::DistMetadataVOL::set_intercomm, "filename"_a, "pattern"_a, "index"_a, "set (filename,pattern) -> intercomm index")
        .def("serve_all",       &LowFive::DistMetadataVOL::serve_all, "serve all datasets")
        .def("get_filenames",   &LowFive::DistMetadataVOL::get_filenames, "intercomm_index"_a, "get filenames produced by producer at intercomm")
        .def("send_done",       &LowFive::DistMetadataVOL::send_done, "intercomm_index"_a, "tell producer that consumer is done, so producer can proceed")
        .def("producer_done",   &LowFive::DistMetadataVOL::producer_signal_done, "tell consumers that producer is done")
    ;
}
