#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
namespace py = pybind11;

#include <lowfive/vol-metadata.hpp>
#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/../../src/log-private.hpp>

PYBIND11_MODULE(_lowfive, m)
{
    using namespace pybind11::literals;

    m.doc() = "LowFive python bindings";

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

    using communicator  = LowFive::DistMetadataVOL::communicator;
    using communicators = LowFive::DistMetadataVOL::communicators;
    py::class_<LowFive::DistMetadataVOL> dist_metadata_vol(m, "DistMetadataVOL", "metadata VOL object", metadata_vol);
    dist_metadata_vol
        .def(py::init<communicator, communicator>(),  "local"_a, "intercomm"_a,  "construct the object")
        .def(py::init<communicator, communicators>(), "local"_a, "intercomms"_a, "construct the object")
        .def_readwrite("serve_on_close",   &LowFive::DistMetadataVOL::serve_on_close)
        .def("set_intercomm",   &LowFive::DistMetadataVOL::set_intercomm, "filename"_a, "pattern"_a, "index"_a, "set (filename,pattern) -> intercomm index")
        .def("serve_all",       &LowFive::DistMetadataVOL::serve_all, "serve all datasets")
        .def("get_filenames",   &LowFive::DistMetadataVOL::get_filenames, "intercomm_index"_a, "get filenames produced by producer at intercomm")
        .def("send_done",       &LowFive::DistMetadataVOL::send_done, "intercomm_index"_a, "tell producer that consumer is done, so producer can proceed")
        .def("producer_done",   &LowFive::DistMetadataVOL::producer_signal_done, "tell consumers that producer is done")
    ;

    py::class_<communicator>(m, "MPIComm")
        .def(py::init<>())
        .def(py::init([](long comm_)
             {
                return new communicator(*static_cast<MPI_Comm*>(reinterpret_cast<void*>(comm_)));
             }))
        //.def_property_readonly("size", &communicator::size)
        //.def_property_readonly("rank", &communicator::rank)
        //.def_property_readonly("comm", &communicator::handle)
    ;


}
