#pragma once

namespace LowFive
{

struct Link : public Object
{
    bool hard;
    std::string target;

    Link(std::string name, bool hard_, std::string target_):
        Object(ObjectType::Link, name), hard(hard_), target(target_)
    {}
};

}
