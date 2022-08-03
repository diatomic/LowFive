#pragma once

#include <algorithm>

#include "index-query.hpp"

#include "rpc/server.h"
#include "dist/core.hpp"


namespace LowFive
{

struct Index: public IndexQuery
{
    using ServeData             = Datasets;            // datasets producer is serving

    IndexedDatasets             index_data; // local data for multiple datasets

    // producer version of the constructor
                        Index(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, const ServeData& serve_data);

    // TODO: index-query are written with the bulk-synchronous assumption;
    //       think about how to make it completely asynchronous
    // NB, index and query are called once for each (DIY) block of the user code, not once per rank
    // The bulk synchronous assumption means that each rank must have the same number of blocks (no remainders)
    // It also means that all ranks in a producer-consumer task pair need to index/query the same number of times
    void                index(IndexedDataset& data);

    void                serve();

    static void         print(int rank, const BoxLocations& boxes);

    void                print();


    IndexServe          idx_srv;
};

}
