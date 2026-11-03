#include "test.hpp"
#include <diy/master.hpp>
#include "metadata.hpp"
#include "rpc/client.h"
#include "rpc/server.h"
#include <array>
#include <cstring>
#include <stdexcept>
#include <vector>

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
    module.function("fail", []() { throw std::runtime_error("intentional server failure"); });
    module.function("reset-buffer", []()
    {
        diy::MemoryBuffer result;
        diy::save(result, 37);
        result.reset();
        return result;
    });
}

void exercise_blob_lifetime(int rank)
{
    std::vector<char> expected(65539);
    for (size_t i = 0; i < expected.size(); ++i)
        expected[i] = static_cast<char>(i % 127);
    const std::array<char, 3> trailer = {{'e', 'n', 'd'}};
    if (rank == 0)
    {
        diy::MemoryBuffer outgoing;
        diy::save(outgoing, 42);
        outgoing.save_binary_blob(expected.data(), expected.size());
        outgoing.save_binary_blob(trailer.data(), trailer.size());
        LowFive::rpc::ssend(MPI_COMM_WORLD, 1, 17, outgoing);
    }
    else
    {
        diy::MemoryBuffer::Blob retained;
        {
            diy::MemoryBuffer incoming;
            LowFive::rpc::recv(MPI_COMM_WORLD, 0, 17, incoming);
            int header = 0;
            diy::load(incoming, header);
            CHECK(header == 42);
            retained = incoming.load_binary_blob();
            auto last = incoming.load_binary_blob();
            CHECK(last.size == trailer.size());
            CHECK(std::memcmp(last.pointer.get(), trailer.data(), last.size) == 0);
        }
        CHECK(retained.size == expected.size());
        CHECK(std::memcmp(retained.pointer.get(), expected.data(), retained.size) == 0);
    }
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
        if (name == "blob-lifetime")
            exercise_blob_lifetime(rank);
        else if (rank == 0)
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
            if (name == "exception")
            {
                bool failed = false;
                try
                {
                    client.call<void>("fail");
                }
                catch (const std::runtime_error& error)
                {
                    failed = std::string(error.what()).find(
                                 "intentional server failure") !=
                             std::string::npos;
                }
                CHECK(failed);
                CHECK(first.call<int>("get") == 17);
                auto buffer = client.call<diy::MemoryBuffer>("reset-buffer");
                int value = 0;
                diy::load(buffer, value);
                CHECK(value == 37);
            }
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
