#pragma once

#include <tuple>
#include <vector>

#include <mpi.h>
#include <diy/mpi.hpp>

#include "../metadata/serialization.hpp"

#include "../log-private.hpp"
#include "operations.h"

namespace LowFive
{

namespace rpc
{

struct server
{
    struct module;
    struct object;      // XXX: unused; not sure why it's here (probably an old oversight); remove if everything works

                    server(module& m, MPI_Comm comm):
                        m_(m), comm_(comm)          {}

    inline int      probe();
    inline bool     process(int rank);

    private:
        module&                 m_;
        diy::mpi::communicator  comm_;
};

}   // namespace rpc
}   // namespace LowFive

#include "modules/server.h"

int
LowFive::rpc::
server::
probe()
{
    diy::mpi::optional<diy::mpi::status> ostatus;
    ostatus = comm_.iprobe(diy::mpi::any_source, tags::consumer);
    if (ostatus)
        return ostatus->source();
    else
        return -1;
}

bool
LowFive::rpc::
server::
process(int source)
{
    auto log = get_logger();

    diy::MemoryBuffer in, out;
    comm_.recv(source, tags::consumer, in.buffer);

    ops::Operation op;
    diy::load(in, op);
    log->trace("Received op: {}", op);

    if (op == ops::function)
    {
        log->trace("Function call");
        m_.call(in, out);
    } else if (op == ops::mem_fn)
    {
        log->trace("Member function call");
        m_.call_mem_fn(in, out);
    } else if (op == ops::create)
    {
        log->trace("Constructor");
        m_.create(in, out);
    } else if (op == ops::destroy)
    {
        log->trace("Destructor");
        m_.destroy(in);
    } else if (op == ops::finish)
        return true;
    else
        throw std::runtime_error("Uknown operation");

    comm_.send(source, tags::producer, out.buffer);

    return false;
}
