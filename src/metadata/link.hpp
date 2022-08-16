#pragma once

namespace LowFive
{

struct HardLink : public Object
{
    Object* target;

    HardLink(std::string name, Object* target_):
        Object(ObjectType::HardLink, name), target(target_)
    {}

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Hard Link ---\n");

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "target = {}\n", target->fullname().second);

        Object::print(depth);
    }
};

struct SoftLink: public Object
{
    std::string target;

    SoftLink(std::string name, std::string target_):
        Object(ObjectType::SoftLink, name), target(target_)
    {}

    void print(int depth) const override
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "---- Soft Link ---\n");

        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "target = {}\n", target);

        Object::print(depth);
    }
};

}
