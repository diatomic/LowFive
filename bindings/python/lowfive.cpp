#include <pybind11/pybind11.h>
namespace py = pybind11;

#include <lowfive/vol-metadata.hpp>

PYBIND11_MODULE(lowfive, m)
{
    using namespace pybind11::literals;

    m.doc() = "LowFive python bindings";

    py::class_<LowFive::VOLBase>(m, "VOLBase", "base VOL object")
        .def(py::init<>(), "construct the object")
    ;

    py::class_<LowFive::MetadataVOL>(m, "MetadataVOL", "metadata VOL object")
        .def(py::init<>(), "construct the object")
        .def("set_passthru", &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",   &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy", &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
    ;
}
