#pragma once

namespace LowFive
{

struct Object
{
    Object*                         parent;
    std::vector<Object*>            children;

    ObjectType                      type;
    std::string                     name;

    Object(ObjectType type_, std::string name_) :
        parent(nullptr),
        type(type_), name(name_)                        {}

    virtual ~Object()
    {
        remove();

        // this assumes that there are no cycles, might not be possible, if we implement HDF5 links
        // might want to switch to shared_ptr<Object> instead
        for (auto* child : children)
        {
            child->parent = nullptr;    // to skip remove() in child
            delete child;
        }
    }

    virtual void print() const
    {
        fmt::print(stderr, "object type = {} name = {}\n", type, name);
        // TODO: print attributes and properties

        for (auto* child : children)
            child->print();
    }

    Object* add_child(Object* object)
    {
        children.push_back(object);
        object->parent = this;
        return object;

        // debug
        fmt::print(stderr, "Added metadata node: name {} type {}\n", object->name, object->type);
    }

    // remove this from parent's children
    void remove()
    {
        if (parent)
        {
            parent->children.erase(std::find(parent->children.begin(), parent->children.end(), this));
            parent = nullptr;
        }
    }
};

}
