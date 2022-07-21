#pragma once

#include <vector>
#include <set>
#include <string>

#include <diy/decomposition.hpp>
#include <diy/types.hpp>

#include <lowfive/metadata.hpp>
#include <lowfive/metadata/serialization.hpp>

namespace LowFive
{

struct IndexedDataset
{
    using Decomposer    = IndexQuery::Decomposer;
    using BoxLocations  = IndexQuery::BoxLocations;
    using Bounds        = IndexQuery::Bounds;

    IndexedDataset(Dataset* ds_, int comm_size):
        ds(ds_)
    {
        dim = ds->space.dims.size();
        type = ds->type;
        space = ds->space;

        Bounds domain { dim };
        domain.max = ds->space.dims;

        decomposer = Decomposer(dim, domain, comm_size);
    }

    auto                metadata()
    {
        return std::make_tuple(dim, type, space);
    }

    Dataset*                ds;
    int                     dim;
    Datatype                type;
    Dataspace               space;
    Decomposer              decomposer { 1, Bounds { { 0 }, { 1} }, 1 };
    BoxLocations            boxes;
};

using IndexedDatasets       = std::map<std::string, IndexedDataset>;

struct IndexServe
{

    void                    file_open()     { ++open_files; }
    void                    file_close()    { --open_files; }
    std::set<std::string>   get_filenames()
    {
        // traverse all indexed datasets and collect dataset filenames in fnames
        std::set<std::string> fnames;

        for(auto key_ds : *index_data)
        {
            std::string fname = key_ds.second.ds->fullname().first;
            fnames.insert(fname);
        }

        return fnames;
    }

    IndexedDatasets*    index_data;
    int                 open_files = 0;
};

template<class module>
void export_core(module& m, IndexServe* idx)
{
    m.template class_<IndexedDataset>("Dataset")
        //.function("metadata", [](IndexedDataset* ids) { return std::make_tuple(ids->dim, ids->type, ids->space); })
        .function("metadata", &IndexedDataset::metadata);
    ;

    //m.function("file_open", [idx]() { idx->file_open(); });
    //m.function("file_close", [idx]() { idx->file_close(); });

    //m.function("dataset_open", [idx](std::string name) { return &(idx->index_data->at(name)); });
    //m.function("dataset_close", []() {});       // nothing for now

    ////m.function("query", &query);

    //m.function("get_filenames", [idx]() { return idx->get_filenames(); });

    //m.function("finish", &finish);
}

}
