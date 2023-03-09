#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
namespace py = pybind11;


#include <lowfive/log.hpp>

#if defined(LOWFIVE_MPI4PY)
#include "mpi-comm.h"
#endif

#include "mpi-capsule.h"

#include <lowfive/vol-metadata.hpp>
#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/../../src/log-private.hpp>

herr_t
fail_on_hdf5_error(hid_t stack_id, void*)
{
    H5Eprint(stack_id, stderr);
    fprintf(stderr, "An HDF5 error was detected. Terminating.\n");
    exit(1);
}

PYBIND11_MODULE(_lowfive, m)
{
    using namespace pybind11::literals;

    m.doc() = "LowFive python bindings";

    // set HDF5 error handler
    H5Eset_auto(H5E_DEFAULT, fail_on_hdf5_error, NULL);

#if defined(LOWFIVE_MPI4PY)
    // import the mpi4py API
    if (import_mpi4py() < 0)
        throw std::runtime_error("Could not load mpi4py API.");
#endif

    m.def("create_logger", [](std::string lev) { LowFive::create_logger(lev); return 0; }, "Create spdlog logger for LowFive");
    m.def("create_MetadataVOL", &LowFive::MetadataVOL::create_MetadataVOL, "Get MetadataVOL object", py::return_value_policy::reference);
    m.def("create_DistMetadataVOL", [](py::capsule local, py::capsule intercomm) -> LowFive::DistMetadataVOL&
                      {
                        return LowFive::DistMetadataVOL::create_DistMetadataVOL(from_capsule<MPI_Comm>(local), from_capsule<MPI_Comm>(intercomm));
                      },  "local"_a, "intercomm"_a,  "construct the object", py::return_value_policy::reference);

    m.def("create_DistMetadataVOL", [](py::capsule local, std::vector<py::capsule> intercomms) -> LowFive::DistMetadataVOL&
                      {
                          MPI_Comm local_ = from_capsule<MPI_Comm>(local);
                          std::vector<MPI_Comm> intercomms_;
                          for (auto& c : intercomms)
                            intercomms_.push_back(from_capsule<MPI_Comm>(c));
                          return LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_);
                      }, "local"_a, "intercomms"_a, "construct the object", py::return_value_policy::reference);

#if defined(LOWFIVE_MPI4PY)
    m.def("create_DistMetadataVOL", [](mpi4py_comm local, mpi4py_comm intercomm) ->LowFive::DistMetadataVOL&
        {
            return LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomm);
        },  "local"_a, "intercomm"_a,  "construct the object", py::return_value_policy::reference);
    m.def("create_DistMetadataVOL", [](mpi4py_comm local, std::vector<mpi4py_comm> intercomms) -> LowFive::DistMetadataVOL&
                      {
                          MPI_Comm local_ = local;
                          std::vector<MPI_Comm> intercomms_(intercomms.begin(), intercomms.end());
                          return LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_);
                      }, "local"_a, "intercomms"_a, "construct the object", py::return_value_policy::reference);
#endif

    py::class_<LowFive::VOLBase> vol_base(m, "VOLBase", "base VOL object");
//    vol_base
//        .def(py::init<>(), "construct the object")
//    ;

    py::class_<LowFive::MetadataVOL> metadata_vol(m, "MetadataVOL", "metadata VOL object", vol_base);
    metadata_vol
        .def("set_passthru",   &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",     &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy",   &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
        .def("set_keep",       &LowFive::MetadataVOL::set_keep,     "keep_a",                  "set whether to keep files in the metadata after they are closed")
        .def("print_files",    &LowFive::MetadataVOL::print_files,                             "print file metadata")
        .def("clear_files",    &LowFive::MetadataVOL::clear_files,                             "clear all files")
        .def("set_after_file_close", &LowFive::MetadataVOL::set_after_file_close,              "set the after_file_close callback")
        .def("set_before_file_open", &LowFive::MetadataVOL::set_before_file_open,              "set the before_file_open callback")
        .def("set_after_dataset_write", &LowFive::MetadataVOL::set_after_dataset_write,        "set the after_dataset_write callback")
    ;

    py::class_<LowFive::DistMetadataVOL> dist_metadata_vol(m, "DistMetadataVOL", "metadata VOL object", metadata_vol);
    dist_metadata_vol
        .def_readwrite("serve_on_close",    &LowFive::DistMetadataVOL::serve_on_close)
        .def_readonly("file_close_counter", &LowFive::DistMetadataVOL::file_close_counter_)
        .def("set_intercomm",   &LowFive::DistMetadataVOL::set_intercomm,           "filename"_a, "pattern"_a, "index"_a,
                                                                                    "set (filename,pattern) -> intercomm index")
        .def("serve_all",       &LowFive::DistMetadataVOL::serve_all,               "serve all datasets")
        .def("get_filenames",   &LowFive::DistMetadataVOL::get_filenames,           "intercomm_index"_a,
                                                                                    "get filenames produced by producer at intercomm")
        .def("send_done",       &LowFive::DistMetadataVOL::send_done,               "intercomm_index"_a,
                                                                                    "tell producer that consumer is done, so producer can proceed")
        .def("producer_done",   &LowFive::DistMetadataVOL::producer_signal_done,    "tell consumers that producer is done")
        .def("broadcast_files", &LowFive::DistMetadataVOL::broadcast_files,         py::arg("root") = 0,
                                                                                    "broadcast file metadata to all ranks")
        .def("set_serve_indices", &LowFive::DistMetadataVOL::set_serve_indices,     "set the serve_indices callback")
    ;
}
