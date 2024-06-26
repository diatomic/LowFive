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

// custom deleter for unique_ptrs for VOLs
struct nodelete_unset_cb {
    template<class T>
    void operator()(T* vol)
    {
        py::gil_scoped_acquire acq;
        vol->unset_callbacks();
    }
};

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

    // type aliases for unique pointers with custom deleters
    // custom deleter will:
    // 1. acquire GIL
    // 2. unset callbacks (so they are destroyed with GIL held)
    // and will NOT delete the object (this is done in _term when HDF5 library is closing)
    using PMetadataVOL = std::unique_ptr<LowFive::MetadataVOL, nodelete_unset_cb>;
    using PDistMetadataVOL = std::unique_ptr<LowFive::DistMetadataVOL, nodelete_unset_cb>;

    m.def("create_logger", [](std::string lev) { LowFive::create_logger(lev); return 0; }, "Create spdlog logger for LowFive");

    m.def("_create_MetadataVOL", []() -> PMetadataVOL
                       {
                            return PMetadataVOL(&LowFive::MetadataVOL::create_MetadataVOL(), nodelete_unset_cb()
                                                );
                       }, "Get MetadataVOL object");
    m.def("_create_DistMetadataVOL", [](py::capsule local, py::capsule intercomm) -> PDistMetadataVOL
                      {
                         return PDistMetadataVOL(&LowFive::DistMetadataVOL::create_DistMetadataVOL(from_capsule<MPI_Comm>(local), from_capsule<MPI_Comm>(intercomm)),
                                                 nodelete_unset_cb());
                      },  "local"_a, "intercomm"_a,  "construct the object");

    m.def("_create_DistMetadataVOL", [](py::capsule local, std::vector<py::capsule> intercomms) -> PDistMetadataVOL
                      {
                          MPI_Comm local_ = from_capsule<MPI_Comm>(local);
                          std::vector<MPI_Comm> intercomms_;
                          for (auto& c : intercomms)
                            intercomms_.push_back(from_capsule<MPI_Comm>(c));
                          return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_), nodelete_unset_cb()};
                      }, "local"_a, "intercomms"_a, "construct the object");

#if defined(LOWFIVE_MPI4PY)
    m.def("_create_DistMetadataVOL", [](mpi4py_comm local, mpi4py_comm intercomm) -> PDistMetadataVOL
        {
            return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomm), nodelete_unset_cb()
                   };
        },  "local"_a, "intercomm"_a,  "construct the object");
    m.def("_create_DistMetadataVOL", [](mpi4py_comm local, std::vector<mpi4py_comm> intercomms) -> PDistMetadataVOL
                      {
                          MPI_Comm local_ = local;
                          std::vector<MPI_Comm> intercomms_(intercomms.begin(), intercomms.end());
                          return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_), nodelete_unset_cb()
                          };
                      }, "local"_a, "intercomms"_a, "construct the object");
#endif

    py::class_<LowFive::VOLBase> vol_base(m, "VOLBase", "base VOL object");

    py::class_<LowFive::MetadataVOL, std::unique_ptr<LowFive::MetadataVOL, nodelete_unset_cb>> metadata_vol(m, "MetadataVOL", "metadata VOL object", vol_base);
    metadata_vol
        .def("set_passthru",            &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",              &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy",            &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
        .def("is_passthru",             &LowFive::MetadataVOL::is_passthru,  "filename"_a, "pattern"_a, "is (filename,pattern) set for passthru")
        .def("is_memory",               &LowFive::MetadataVOL::is_memory,    "filename"_a, "pattern"_a, "is(filename,pattern) set for memory")
        .def("is_zerocopy",             &LowFive::MetadataVOL::is_zerocopy,  "filename"_a, "pattern"_a, "is (filename,pattern) set for zerocopy")
        .def("set_keep",                &LowFive::MetadataVOL::set_keep,     "keep_a",                  "set whether to keep files in the metadata after they are closed")
        .def("print_files",             &LowFive::MetadataVOL::print_files,                             "print file metadata")
        .def("clear_files",             &LowFive::MetadataVOL::clear_files,                             "clear all files")
        // to set/unset callbacks, we need to hold GIL in lambda
        .def("set_after_file_close",    [](LowFive::MetadataVOL* self,  LowFive::MetadataVOL::AfterFileClose& afc)
                                        {
                                            py::gil_scoped_acquire acq;
                                            self->set_after_file_close(afc);
                                        },                                                              "set the after_file_close callback")
        .def("set_before_file_open",    [](LowFive::MetadataVOL* self,  LowFive::MetadataVOL::BeforeFileOpen& bfo)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_before_file_open(bfo);
                                        },                                                              "set the before_file_open callback")
        .def("set_after_dataset_write",    [](LowFive::MetadataVOL* self,  LowFive::MetadataVOL::AfterDatasetWrite& adw)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_after_dataset_write(adw);
                                        },                                                              "set the after_dataset_write callback")
        .def("unset_callbacks",        [](LowFive::MetadataVOL* self)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->unset_callbacks();
                                        },                                                              "set the after_dataset_write callback")
    ;

    py::class_<LowFive::DistMetadataVOL, std::unique_ptr<LowFive::DistMetadataVOL, py::nodelete>> dist_metadata_vol(m, "DistMetadataVOL", "metadata VOL object", metadata_vol);
    dist_metadata_vol
        .def_readwrite("serve_on_close",              &LowFive::DistMetadataVOL::serve_on_close)
        .def_readonly("file_close_counter",           &LowFive::DistMetadataVOL::file_close_counter_)
        .def("set_intercomm",   &LowFive::DistMetadataVOL::set_intercomm,           "filename"_a, "pattern"_a, "index"_a,
                                                                                    "set (filename,pattern) -> intercomm index")
        .def("serve_all",       &LowFive::DistMetadataVOL::serve_all,               "delete_data"_a=true, "perform_indexing"_a=true, "serve all datasets")
        .def("get_filenames",   &LowFive::DistMetadataVOL::get_filenames,           "intercomm_index"_a,
                                                                                    "get filenames produced by producer at intercomm")
        .def("send_done",       &LowFive::DistMetadataVOL::send_done,               "intercomm_index"_a,
                                                                                    "tell producer that consumer is done, so producer can proceed")
        .def("producer_done",   &LowFive::DistMetadataVOL::producer_signal_done,    "tell consumers that producer is done")
        .def("broadcast_files", &LowFive::DistMetadataVOL::broadcast_files,         py::arg("root") = 0,
                                                                                    "broadcast file metadata to all ranks")

        // in the following functions we need to  hold GIL
        .def("set_serve_indices",       [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::ServeIndices& si)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_serve_indices(si);
                                        },                                                              "set the serve_file_indices callback")
        .def("set_consumer_filename",   [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::SetFileName& sfn)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_consumer_filename(sfn);
                                        },                                                              "set the consumer_filename callback")
        .def("set_send_filename",       [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::SendFileName& sfn)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_send_filename(sfn);
                                        },                                                              "set the send_filename callback")
        .def("set_noncollective_datasets", [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::NoncollectiveDatasets& nd)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_noncollective_datasets(nd);
                                        },                                                              "set the noncollective_datasets callback")

        .def("unset_dist_callbacks",    [](LowFive::DistMetadataVOL* self)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->unset_dist_callbacks();
                                        },                                                              "unset DistMetadataVOL callbacks")

    ;
}
