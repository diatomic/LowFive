#include <pybind11/pybind11.h>
namespace py = pybind11;

#include <lowfive/vol-metadata.hpp>
#include <lowfive/vol-dist-metadata.hpp>

PYBIND11_MODULE(_lowfive, m)
{
    using namespace pybind11::literals;

    m.doc() = "LowFive python bindings";

    py::class_<LowFive::VOLBase>(m, "VOLBase", "base VOL object")
        .def(py::init<>(), "construct the object")
    ;

    py::class_<LowFive::MetadataVOL>(m, "MetadataVOL", "metadata VOL object")
        .def(py::init<>(), "construct the object")
        .def("set_passthru",   &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",     &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy",   &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
        .def("set_keep",       &LowFive::MetadataVOL::set_keep,     "keep_a",                  "set whether to keep files in the metadata after they are closed")
    ;

    using communicator  = LowFive::DistMetadataVOL::communicator;
    using communicators = LowFive::DistMetadataVOL::communicators;
    py::class_<LowFive::DistMetadataVOL>(m, "DistMetadataVOL", "metadata VOL object")
        .def(py::init<communicator, communicator>(),  "local"_a, "intercomm"_a,  "construct the object")
        .def(py::init<communicator, communicators>(), "local"_a, "intercomms"_a, "construct the object")
        .def("set_intercomm",   &LowFive::DistMetadataVOL::set_intercomm, "filename"_a, "pattern"_a, "index"_a, "set (filename,pattern) -> intercomm index")
        .def("serve_all",       &LowFive::DistMetadataVOL::serve_all, "serve all datasets")
    ;
}
