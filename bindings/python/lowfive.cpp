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

//struct PyVOLBase
//{
//    LowFive::VOLBase* vol_ {nullptr};
//};
//
//struct PyMetadataVOL: public PyVOLBase
//{
//    void set_passthru(std::string filename, std::string pattern)
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_passthru(filename, pattern);
//    }
//
//    void set_memory(std::string filename, std::string pattern)
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_memory(filename, pattern);
//    }
//
//    void set_zerocopy(std::string filename, std::string pattern)
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_zerocopy(filename, pattern);
//    }
//
//    void set_keep(bool keep_)
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_keep(keep_);
//    }
//
//    void print_files()
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->print_files();
//    }
//
//    void clear_files()
//    {
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->clear_files();
//    }
//
//    void set_after_file_close(LowFive::MetadataVOL::AfterFileClose afc)
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_after_file_close(afc);
//    }
//
//    void set_before_file_open(LowFive::MetadataVOL::BeforeFileOpen bfo)
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_before_file_open(bfo);
//    }
//
//    void set_after_dataset_write(LowFive::MetadataVOL::AfterDatasetWrite adw)
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->set_after_dataset_write(adw);
//    }
//
//    void unset_callbacks()
//    {
////        std::cerr << "PyMetadataVOL::unset_callbacks called" << std::endl;
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->unset_callbacks();
//    }
//
//    ~PyMetadataVOL()
//    {
//        py::gil_scoped_acquire acq;
////        std::cerr << "~PyMetadataVOL: unsetting callbacks" << std::endl;
//        dynamic_cast<LowFive::MetadataVOL*>(vol_)->unset_callbacks();
//    }
//};
//
//struct PyDistMetadataVOL: public PyMetadataVOL
//{
//    void set_intercomm(std::string filename, std::string full_path, int intercomm_index)
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->set_intercomm(filename, full_path, intercomm_index);
//    }
//
//    void serve_all(bool delete_data = true, bool perform_indexing = true)
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->serve_all(delete_data, perform_indexing);
//    }
//
//    decltype(auto)  get_filenames(int intercomm_index)
//    {
//        return dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->get_filenames(intercomm_index);
//    }
//
//    void send_done(int intercomm_index)
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->send_done(intercomm_index);
//    }
//
//    void producer_done()
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->producer_signal_done();
//    }
//
//    void broadcast_files(int root = 0)
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->broadcast_files(root);
//    }
//
//    void set_serve_indices(LowFive::DistMetadataVOL::ServeIndices si)
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->set_serve_indices(si);
//    }
//
//    void set_consumer_filename(LowFive::DistMetadataVOL::SetFileName name)
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->set_consumer_filename(name);
//    }
//
//    bool get_serve_on_close() const
//    {
//        return dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->serve_on_close;
//    }
//
//    void set_serve_on_close(bool value)
//    {
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->serve_on_close = value;
//    }
//
//    decltype(auto) get_file_close_counter() const
//    {
//        return dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->file_close_counter_;
//    }
//
//    void unset_dist_callbacks()
//    {
//        py::gil_scoped_acquire acq;
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->unset_dist_callbacks();
//    }
//
//    ~PyDistMetadataVOL()
//    {
//        py::gil_scoped_acquire acq;
//        //std::cerr << "~PyDistMetadataVOL: unsetting callbacks" << std::endl;
//        dynamic_cast<LowFive::DistMetadataVOL*>(vol_)->unset_dist_callbacks();
//    }
//};


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
//    using PMetadataVOL = std::unique_ptr<LowFive::MetadataVOL, void(*)(LowFive::MetadataVOL*)>;
    using PDistMetadataVOL = std::unique_ptr<LowFive::DistMetadataVOL, void(*)(LowFive::DistMetadataVOL*)>;
    using PMetadataVOL = std::unique_ptr<LowFive::MetadataVOL>;
//    using PDistMetadataVOL = std::unique_ptr<LowFive::DistMetadataVOL>;

    m.def("create_logger", [](std::string lev) { LowFive::create_logger(lev); return 0; }, "Create spdlog logger for LowFive");

    m.def("_create_MetadataVOL", []() -> PMetadataVOL
                       {
                            std::cerr << "enter _create_MetadataVOL" << std::endl;
                            return PMetadataVOL(&LowFive::MetadataVOL::create_MetadataVOL()
//                                                ,[](LowFive::MetadataVOL* vol)
//                                                {
//                                                    std::cerr << "custom deleter" << std::endl;
//                                                    py::gil_scoped_acquire acq;
//                                                    vol->unset_callbacks();
//                                                }
                                                );
                       }, "Get MetadataVOL object");
    m.def("_create_DistMetadataVOL", [](py::capsule local, py::capsule intercomm) -> PDistMetadataVOL
                      {
                         return PDistMetadataVOL(&LowFive::DistMetadataVOL::create_DistMetadataVOL(from_capsule<MPI_Comm>(local), from_capsule<MPI_Comm>(intercomm)),
                                                 [](LowFive::DistMetadataVOL* vol)
                                                 {
                                                    std::cerr << "custom deleter" << std::endl;
                                                    py::gil_scoped_acquire acq;
                                                    vol->unset_callbacks();
                                                 }
                         );
                      },  "local"_a, "intercomm"_a,  "construct the object");

    m.def("_create_DistMetadataVOL", [](py::capsule local, std::vector<py::capsule> intercomms) -> PDistMetadataVOL
                      {
                          MPI_Comm local_ = from_capsule<MPI_Comm>(local);
                          std::vector<MPI_Comm> intercomms_;
                          for (auto& c : intercomms)
                            intercomms_.push_back(from_capsule<MPI_Comm>(c));
                          return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_),
                                  [](LowFive::DistMetadataVOL* vol)
                                  {
                                    std::cerr << "custom deleter" << std::endl;
                                    py::gil_scoped_acquire acq;
                                    vol->unset_callbacks();
                                  }
                                  };
                      }, "local"_a, "intercomms"_a, "construct the object");

#if defined(LOWFIVE_MPI4PY)
    m.def("_create_DistMetadataVOL", [](mpi4py_comm local, mpi4py_comm intercomm) -> PDistMetadataVOL
        {
            return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomm),
                    [](LowFive::DistMetadataVOL* vol)
                    {
                      std::cerr << "custom deleter" << std::endl;
                      py::gil_scoped_acquire acq;
                      vol->unset_callbacks();
                    }
                   };
        },  "local"_a, "intercomm"_a,  "construct the object");
    m.def("_create_DistMetadataVOL", [](mpi4py_comm local, std::vector<mpi4py_comm> intercomms) -> PDistMetadataVOL
                      {
                          MPI_Comm local_ = local;
                          std::vector<MPI_Comm> intercomms_(intercomms.begin(), intercomms.end());
                          return {&LowFive::DistMetadataVOL::create_DistMetadataVOL(local_, intercomms_),
                                  [](LowFive::DistMetadataVOL* vol)
                                  {
                                    std::cerr << "custom deleter" << std::endl;
                                    py::gil_scoped_acquire acq;
                                    vol->unset_callbacks();
                                  }
                          };
                      }, "local"_a, "intercomms"_a, "construct the object");
#endif

    py::class_<LowFive::VOLBase> vol_base(m, "VOLBase", "base VOL object");

    py::class_<LowFive::MetadataVOL> metadata_vol(m, "MetadataVOL", "metadata VOL object", vol_base);
    metadata_vol
        .def("set_passthru",            &LowFive::MetadataVOL::set_passthru, "filename"_a, "pattern"_a, "set (filename,pattern) for passthru")
        .def("set_memory",              &LowFive::MetadataVOL::set_memory,   "filename"_a, "pattern"_a, "set (filename,pattern) for memory")
        .def("set_zerocopy",            &LowFive::MetadataVOL::set_zerocopy, "filename"_a, "pattern"_a, "set (filename,pattern) for zerocopy")
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

    py::class_<LowFive::DistMetadataVOL> dist_metadata_vol(m, "DistMetadataVOL", "metadata VOL object", metadata_vol);
    dist_metadata_vol
        .def_readwrite("serve_on_close",              &LowFive::DistMetadataVOL::serve_on_close)
        .def_readonly("file_close_counter",           &LowFive::DistMetadataVOL::file_close_counter_)
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

        // in the following functions we need to  hold GIL
        .def("set_serve_indices",       [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::ServeIndices& si)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_serve_indices(si);
                                        },                                                              "set the serve_file_indices callback")

        .def("unset_dist_callbacks",    [](LowFive::DistMetadataVOL* self)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->unset_dist_callbacks();
                                        },                                                              "unset DistMetadataVOL callbacks")
        .def("set_consumer_filename",   [](LowFive::DistMetadataVOL* self,  LowFive::DistMetadataVOL::SetFileName& sfn)
                                        {
                                          py::gil_scoped_acquire acq;
                                          self->set_consumer_filename(sfn);
                                        },                                                              "set the consumer_filename callback")
    ;
}
