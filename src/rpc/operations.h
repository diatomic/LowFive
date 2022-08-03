#pragma once

namespace LowFive
{
namespace rpc
{
namespace ops
{

enum Operation { finish, function, mem_fn, create, destroy };

}

// tags indicate the source of communication, so that in the threaded
// regime, they can be used to correctly distinguish between senders and
// receipients
enum tags   {
                producer = 1,       // communication from producer
                consumer            // communication from consumer
            };
}
}
