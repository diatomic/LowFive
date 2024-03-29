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

    IndexedDataset(Dataset* ds_, const diy::mpi::communicator& comm):
        ds(ds_)
    {
        dim = ds->space.dims.size();
        type = ds->type;
        space = ds->space;

        Bounds domain { dim };
        domain.max = ds->space.dims;

        // TODO: perhaps check for the size(domain) vs #ranks condition explcitly, instead of this try-catch yoga
        auto log = get_logger();
        try {
            log->trace("Creating decomposer: dim = {}, domain = [{},{}], comm.size() = {}",
                    dim, domain.min, domain.max, comm.size());
            decomposer = Decomposer(dim, domain, comm.size());
        } catch (std::runtime_error&) {
            // assume we are in size(domain) < #ranks
            log->warn("{} = size(domain) < #ranks = {}", domain.max, comm.size());

            // DM (2024-01-23): this used to set the correct decomposer on rank
            // 0 and return a dummy everywhere else. This broke logic for
            // consumers that received a dummy. Now we return the simple
            // decomposer everywhere, which will redirect everybody to rank 0.
            // This seems like reasonable logic, but leaving the old code in
            // place (commented out), just in case we have to revert.

            decomposer = Decomposer(dim, domain, 1);

            //// for now just give all data to rank 0, other ranks get nothing
            //if (comm.rank() == 0) {
            //    // give nblocks = 1 so that this rank takes the whole domain
            //    decomposer = Decomposer(dim, domain, 1);
            //} else {
            //    decomposer = Decomposer(1, Bounds({0}, {1}), 1);
            //    // to signal that this decomposer is dummy
            //    decomposer.dim = decomposer.nblocks = 0;
            //} ;
        }

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
        CALI_CXX_MARK_FUNCTION;
        auto log = get_logger();
        log->trace("In IndexedDataset::get_data(): {}", fs);

        diy::MemoryBuffer queue;

        size_t count_calls = 0;
        bool is_var_length_string = ds->type.is_var_length_string();
        for (auto& y : ds->data)
        {
            if (y.file.intersects(fs))
            {
                Dataspace file_src(Dataspace::project_intersection(y.file.id, y.file.id,   fs.id), true);
                Dataspace mem_src (Dataspace::project_intersection(y.file.id, y.memory.id, fs.id), true);
                CALI_MARK_BEGIN("diy::save");
                diy::save(queue, file_src);
                if (is_var_length_string)
                {
                    Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                    {
                      size_t length = len / sizeof(intptr_t);
                      intptr_t* from = (intptr_t*) ((char*) y.data + loc);
                      for (size_t i = 0; i < length; ++i)
                      {
                        diy::save(queue, ds->strings[from[i]]);
                        ++count_calls;
                      }
                    });
                } else
                {
                    Dataspace::iterate(mem_src, y.type.dtype_size, [&](size_t loc, size_t len)
                    {
                      diy::save(queue, (char*) y.data + loc, len);
                      ++count_calls;
                    });
                }
                CALI_MARK_END("diy::save");
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

                                IndexServe(Files* files_, MetadataVOL* vol_):
                                    files(files_), vol(vol_)       {}


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
        serialize(result, files->at(name), *vol);
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
    MetadataVOL*        vol;
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
