#pragma once

#include <algorithm>

namespace LowFive
{

struct Object
{
    Object*                         parent;
    std::vector<Object*>            children;

    ObjectType                      type;
    std::string                     name;

    struct ObjectPath
    {
        Object*     obj;
        std::string path;

        bool        is_name() const { return path.find('/') == std::string::npos; }
        Object*     exact() const
        {
            if (!path.empty())
                throw MetadataError(fmt::format("Expected to find exact object, but got remainder: {}", path));
            return obj;
        }
    };

    Object(ObjectType   type_,
           std::string  name_):
        parent(nullptr),
        type(type_),
        name(name_)
    {}

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

    virtual void print(int depth) const
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "    ");
        fmt::print(stderr, "object type = {} name = {}\n", type, name);

        for (auto* child : children)
            child->print(depth + 1);
    }

    virtual void fill_token(H5O_token_t& token)
    {
        for (size_t i = 0; i < H5O_MAX_TOKEN_SIZE; ++i)
            token.__data[i] = 0;

        void* _this = this;
        memcpy(token.__data, &_this, sizeof(void*));

        fmt::print(stderr, "fill_token: {} ", fmt::ptr(this));
        for (size_t i = 0; i < H5O_MAX_TOKEN_SIZE; ++i)
            fmt::print(stderr, "{0:x}", token.__data[i]);
        fmt::print(stderr, "\n");
    }

    virtual ObjectPath search(const char* cur_path_)
    {
        std::string cur_path(cur_path_);
        return search(cur_path);
    }

    virtual ObjectPath locate(const H5VL_loc_params_t& loc_params)
    {
        if (loc_params.type == H5VL_OBJECT_BY_SELF)
            return ObjectPath { this, "" };
        else if (loc_params.type == H5VL_OBJECT_BY_NAME)
            return search(loc_params.loc_data.loc_by_name.name);
        else
            throw MetadataError(fmt::format("don't know how to locate by loc_params.type = {}", loc_params.type));
    }

    // search for the object at the leaf of the current path in the subtree rooted at this object
    // this object can be either the root of the current path or its direct parent
    // returns pointer to object and remainder of the path
    virtual ObjectPath search(std::string path)
    {
        if (path.empty())
            return ObjectPath { this, path };

        auto pos = path.find("/");
        std::string first_name, remainder;
        if (pos != std::string::npos)
        {
            first_name = path.substr(0, pos);
            remainder = path.substr(pos + 1);
        }
        else
        {
            first_name = path;
            remainder = "";
        }

        if (first_name.empty())     // path started with /
            return search(remainder);

        for(auto* child : children)
            if (child->name == first_name)
                return child->search(remainder);

        return ObjectPath { this, path };
    }

    std::pair<std::string, std::string> fullname(std::string child_path = "")     // returns filename, full path pair
    {
        std::string full_path;

        Object* o = this;

        while (o->type != ObjectType::File)
        {
            full_path = "/" + o->name + full_path;
            o = o->parent;
        }

        full_path += ((!child_path.empty() && child_path[0] != '/')? "/" : "") + child_path;

        return std::make_pair(o->name, full_path);
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

    Object* find_root()
    {
        Object* cur = this;
        while (cur->parent)
            cur = cur->parent;
        return cur;
    }
};

}
