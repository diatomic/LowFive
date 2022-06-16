#pragma once

#include "index-query.hpp"
#include "lowfive/vol-dist-metadata.hpp"

namespace LowFive
{

struct Query: public IndexQuery
{
    int                         id;
    int                         dim;
    Datatype                    type;
    Dataspace                   space;
    Decomposer                  decomposer { 1, Bounds { { 0 }, { 1} }, 1 };        // dummy, overwritten in the constructor
    int                         remote_size;                                        // remote size of the intercomm at intercomm_index
    int                         intercomm_index;                                    // index of intercomm to use

    // consumer versions of the constructor

    Query(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, int remote_size_, int intercomm_index_ = 0);

    void                file_open();

    void                file_close();

    void dataset_open(std::string name);

    void                dataset_close();

    void                query(const Dataspace&                     file_space,      // input: query in terms of file space
                              const Dataspace&                     mem_space,       // ouput: memory space of resulting data
                              void*                                buf);            // output: resulting data, allocated by caller

    DistMetadataVOL::FileNames get_filenames();

    void send_done();
};

}
