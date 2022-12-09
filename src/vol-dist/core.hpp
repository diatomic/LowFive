#pragma once

#include <vector>
#include <set>
#include <string>

#include <diy/decomposition.hpp>
#include <diy/types.hpp>

#include "../vol-metadata-private.hpp"

#include "../metadata.hpp"
#include "../metadata/serialization.hpp"
#include "../metadata/remote.hpp"

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

        size_t count_calls = 0;
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
                  ++count_calls;
                });
            }
        }

        log->trace("In IndexedDatasets::get_data(): serialized using {} calls", count_calls);
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
    using Files = MetadataVOL::Files;

                                IndexServe(Files* files_):
                                    files(files_)       {}


    File*                       file_open(std::string name)
    {
        auto log = get_logger();
        log->trace("IndexServe::file_open({})", name);

        File* f = dynamic_cast<File*>(files->at(name));
        if (!f)
        {
            log->error("{} is not a File", name);
            throw std::runtime_error(fmt::format("Cannot open {}", name));
        }
        ++open_files;

        return f;
    }

    diy::MemoryBuffer           get_file_hierarchy(std::string name)
    {
        diy::MemoryBuffer result;
        serialize(result, files->at(name));
        return result;
    }

    void                        file_close(File* f)
    {
        auto log = get_logger();
        log->trace("IndexServe::file_close({})", f->name);

        --open_files;
        if (open_files == 0)
            done = true;
    }

    std::vector<std::string>    get_filenames()
    {
        std::vector<std::string> result;
        for (auto& x : *files)      // Files is a map, so keys are already sorted
            result.emplace_back(x.first);
        return result;
    }

    Files*              files;
    int                 open_files = 0;
    bool                done = false;
};

// same function for files and groups
inline Dataset* dataset_open(Object* parent, std::string name)
{
    auto log = get_logger();
    Object* obj = parent->search(name).exact();
    Dataset* ds = dynamic_cast<Dataset*>(obj);
    if (!ds)
    {
        log->error("{} is not a Dataset", name);
        throw std::runtime_error(fmt::format("Cannot open {}", name));
    }

    return ds;
}

template<class module>
void export_core(module& m, IndexServe* idx)
{
    // register different metadata objects
    m.template class_<Dataset>("Dataset")
        .function("metadata",   [](Dataset* ds)
         {
            IndexedDataset* ids = static_cast<IndexedDataset*>(ds->extra);
            return std::make_tuple(ids->dim, ids->type, ids->space, ids->decomposer);
         })
    ;

    m.template class_<File>("File")
        .function("dataset_open", [](File* f, std::string name) { return dataset_open(f, name); })
        .function("file_close",   [idx](File* f)                { idx->file_close(f); })
    ;

    m.template class_<IndexedDataset>("IndexedDataset")
        .function("get_data",   &IndexedDataset::get_data)
        .function("redirects",  &IndexedDataset::redirects)
    ;

    m.function("open_indexed_dataset", [idx](std::pair<std::string, std::string> fullpath)
    {
        auto log = get_logger();
        log->trace("Opening indexed dataset: {} {}", fullpath.first, fullpath.second);

        File* f = dynamic_cast<File*>(idx->files->at(fullpath.first));
        if (!f)
            throw std::runtime_error(fmt::format("Cannot open file {}", fullpath.first));
        Object* o = f->search(fullpath.second).exact();
        if (!dynamic_cast<Dataset*>(o))
            throw std::runtime_error(fmt::format("Cannot open indexed dataset {} {}", fullpath.first, fullpath.second));
        IndexedDataset* ids = (IndexedDataset*) o->extra;
        return ids;
    });

    m.function("file_open",             [idx](std::string name) { return idx->file_open(name); });
    m.function("get_file_hierarchy",    [idx](std::string name) { return idx->get_file_hierarchy(name); });

    m.function("get_filenames", [idx]() { return idx->get_filenames(); });

    //m.function("finish", &finish);
}

}
