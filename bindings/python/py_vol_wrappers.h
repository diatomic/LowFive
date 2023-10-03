#pragma once

#include <vector>

#include <pybind11/pybind11.h>
namespace py = pybind11;

#include <lowfive/log.hpp>

#include <lowfive/vol-metadata.hpp>
#include <lowfive/vol-dist-metadata.hpp>
#include <lowfive/../../src/log-private.hpp>

struct PyVOLBase
{
    LowFive::VOLBase* vol_ {nullptr};
};

struct PyMetadataVOL: public PyVOLBase
{
    void set_passthru(std::string filename, std::string pattern);
    void set_memory(std::string filename, std::string pattern);
    void set_zerocopy(std::string filename, std::string pattern);
    void set_keep(bool keep_);
    void print_files();
    void clear_files();
    void set_after_file_close(LowFive::MetadataVOL::AfterFileClose afc);
    void set_before_file_open(LowFive::MetadataVOL::BeforeFileOpen bfo);
    void set_after_dataset_write(LowFive::MetadataVOL::AfterDatasetWrite adw);
    void unset_callbacks();
    ~PyMetadataVOL();
};

struct PyDistMetadataVOL: public PyMetadataVOL
{
    void set_intercomm(std::string filename, std::string full_path, int intercomm_index);
    void serve_all(bool delete_data = true, bool perform_indexing = true);
    std::vector<std::string> get_filenames(int intercomm_index);
    void send_done(int intercomm_index);
    void producer_done();
    void broadcast_files(int root = 0);
    void set_serve_indices(LowFive::DistMetadataVOL::ServeIndices si);
    void set_consumer_filename(LowFive::DistMetadataVOL::SetFileName name);
    bool get_serve_on_close() const;
    void set_serve_on_close(bool value);
    decltype(LowFive::DistMetadataVOL::file_close_counter_) get_file_close_counter() const;
    void unset_dist_callbacks();
    ~PyDistMetadataVOL();
};

