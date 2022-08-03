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

    Bounds                  domain()        { return decomposer.domain; }

    BoxLocations            redirects(Dataspace ds)
    {
        BoxLocations redirects;
        for (auto& y : boxes)
        {
            auto& ds2 = std::get<0>(y);
            if (ds.intersects(ds2))
                redirects.push_back(y);
        }
        return redirects;
    }

    diy::MemoryBuffer       get_data(Dataspace fs)
    {
        auto log = get_logger();
        log->trace("In IndexedDataset::get_data(): {}", fs);

        diy::MemoryBuffer queue;

        for (auto& y : ds->data)
        {
            if (y.file.intersects(fs))
            {
                Dataspace file_src(Dataspace::project_intersection(y.file.id, y.file.id,   fs.id), true);
                Dataspace mem_src (Dataspace::project_intersection(y.file.id, y.memory.id, fs.id), true);
                diy::save(queue, file_src);
                Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                {
                  diy::save(queue, (char*) y.data + loc, len);
                });
            }
        }

        log->trace("In IndexedDatasets::get_data(): returning queue.size() = {}", queue.size());

        return queue;
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

    void                        file_open()     { ++open_files; }
    void                        file_close()
    {
        auto log = get_logger();
        log->trace("IndexServe::file_close()");

        --open_files;
        if (open_files == 0)
            done = true;
    }
    std::vector<std::string>    get_filenames()
    {
        // traverse all indexed datasets and collect dataset filenames in fnames
        std::set<std::string> fnames;
        for(auto key_ds : *index_data)
        {
            std::string fname = key_ds.second.ds->fullname().first;
            fnames.insert(fname);
        }

        std::vector<std::string> result;
        for (auto& fn : fnames)
            result.emplace_back(fn);

        return result;
    }

    IndexedDatasets*    index_data;
    int                 open_files = 0;
    bool                done = false;
};

template<class module>
void export_core(module& m, IndexServe* idx)
{
    m.template class_<IndexedDataset>("IndexedDataset")
        .function("metadata",   [](IndexedDataset* ids) { return std::make_tuple(ids->dim, ids->type, ids->space); })
        .function("domain",     &IndexedDataset::domain)
        .function("redirects",  &IndexedDataset::redirects)
        .function("get_data",   &IndexedDataset::get_data)
    ;

    m.function("file_open", [idx]() { idx->file_open(); });
    m.function("file_close", [idx]() { idx->file_close(); });

    m.function("dataset_open", [idx](std::string name) { return &(idx->index_data->at(name)); });
    m.function("dataset_close", []() {});       // nothing for now

    m.function("get_filenames", [idx]() { return idx->get_filenames(); });

    //m.function("finish", &finish);
}

}
