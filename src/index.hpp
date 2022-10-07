#pragma once

#include <algorithm>
#include <map>
#include <string>

#include "index-query.hpp"

#include "rpc/server.h"
#include "vol-dist/core.hpp"


namespace LowFive
{

struct Index: public IndexQuery
{
    using Files = MetadataVOL::Files;
    using Datasets = std::map<std::string, Dataset*>;

    // producer version of the constructor
                        Index(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, Files* files);
                        ~Index();

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous
    // NB, index and query are called once for each (DIY) block of the user code, not once per rank
    // The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
    // It also means that all ranks in a producer-consumer task pair need to index/query the same number of times
    void                index(IndexedDataset& data);

    void                serve();

    static void         print(int rank, const BoxLocations& boxes);

    Datasets            find_datasets(File* f);
    void                find_datasets(Object* o, std::string name, Datasets& result);

    IndexServe          idx_srv;
    size_t              indexed_datasets = 0;
};

}
