#include "test.hpp"
#include <diy/master.hpp>
#include "metadata.hpp"
#include "rpc/client.h"
#include "rpc/server.h"

namespace
{
int destroyed = 0;
struct Value
{
    explicit Value(int n): number(n) {}
    ~Value() { ++destroyed; }
    int get() { return number; }
    int number;
};
template<class Module>
void exports(Module& module)
{
    module.template class_<Value>("Value").template constructor<int>().function("get", &Value::get);
    module.function("destroyed", []() { return destroyed; });
    module.function("echo", [](int n) { return n; });
}
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int rank = 0, size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    try
    {
        CHECK(argc == 2 && size == 2);
        const std::string name(argv[1]);
        if (rank == 0)
        {
            LowFive::rpc::server::module module;
            exports(module);
            LowFive::rpc::server server(module, MPI_COMM_WORLD);
            while (!server.process(1)) {}
            CHECK(destroyed == (name == "destroy-second" ? 2 : 1));
        }
        else
        {
            LowFive::rpc::client::module module;
            exports(module);
            LowFive::rpc::client client(module, MPI_COMM_WORLD, 0);
            auto first = client.create("Value", 17);
            CHECK(first.call<int>("get") == 17);
            if (name == "destroy-second")
            {
                auto second = client.create("Value", 29);
                CHECK(second.call<int>("get") == 29);
                second.destroy();
                CHECK(client.call<int>("destroyed") == 1);
                CHECK(first.call<int>("get") == 17);
            }
            first.destroy();
            CHECK(client.call<int>("echo", 73) == 73);
            CHECK(client.call<int>("destroyed") == (name == "destroy-second" ? 2 : 1));
            CHECK(client.call<int>("echo", -9) == -9);
            client.finish(0);
        }
        MPI_Finalize();
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::fprintf(stderr, "rank %d: %s\n", rank, error.what());
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }
}
