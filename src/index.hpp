#pragma once

#include <algorithm>

#include "index-query.hpp"


namespace LowFive
{

struct Index: public IndexQuery
{
    struct IndexedDataset
    {
        IndexedDataset(Dataset* ds_, int comm_size);

        Dataset*                ds;
        int                     dim;
        Datatype                type;
        Dataspace               space;
        Decomposer              decomposer { 1, Bounds { { 0 }, { 1} }, 1 };
        BoxLocations            boxes;
    };
    using IndexedDatasets       = std::map<std::string, IndexedDataset>;
    using IDsMap                = std::map<std::string, int>;
    using IDsVector             = std::vector<std::string>;
    using ServeData             = Datasets;            // datasets producer is serving

    IndexedDatasets             index_data; // local data for multiple datasets
    IDsMap                      ids_map;
    IDsVector                   ids_vector;

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
};

}
