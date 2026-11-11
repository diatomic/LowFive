#include "test.hpp"
#include "vol-metadata-private.hpp"
#include <memory>

namespace
{
void invalid_type(bool nested)
{
    test::Connector connector(nested ? "wire-invalid-child" : "wire-invalid-root");
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    diy::MemoryBuffer buffer;
    const auto header = [&](std::uintptr_t token, LowFive::ObjectType type,
                            const std::string& name, size_t children)
    {
        diy::save(buffer, token);
        diy::save(buffer, type);
        diy::save(buffer, name);
        diy::save(buffer, children);
    };
    if (nested) header(1, LowFive::ObjectType::Group, "parent", 1);
    header(2, static_cast<LowFive::ObjectType>(255), "invalid", 0);
    buffer.reset();
    bool rejected = false;
    try
    {
        std::unique_ptr<LowFive::Object> object(LowFive::deserialize(buffer, vol));
    }
    catch (const LowFive::MetadataError&) { rejected = true; }
    CHECK(rejected);
}
}

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        CHECK(name == "invalid-root" || name == "invalid-child");
        invalid_type(name == "invalid-child");
    });
}
