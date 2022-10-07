#pragma once

#include "index-query.hpp"
#include "lowfive/vol-dist-metadata.hpp"

#include "rpc/client.h"

namespace LowFive
{

struct Query: public IndexQuery
{
    int                         remote_size;                                        // remote size of the intercomm at intercomm_index
    int                         intercomm_index;                                    // index of intercomm to use

    // consumer versions of the constructor

                        Query(MPI_Comm local_, std::vector<MPI_Comm> intercomms_, int remote_size_, int intercomm_index_ = 0);

    DistMetadataVOL::FileNames
                        get_filenames();

    void                send_done();

    rpc::client::module m;
    rpc::client         c;
};

}
