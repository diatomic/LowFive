#pragma once

#include <algorithm>
#include "objecttype.hpp"
#include "error.hpp"
#include "../log-private.hpp"

namespace LowFive
{

struct Object
{
    Object*                         parent;
    std::vector<Object*>            children;
    std::uintptr_t                  token;

    ObjectType                      type;
    std::string                     name;

    void*                           extra = nullptr;        // currently only used to store IndexedDataset for each Dataset

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
    {
        token = reinterpret_cast<std::uintptr_t>(this);
    }

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
        children.clear();
    }

    static void print_depth(int depth)
    {
        for (auto i = 0; i < depth; i++)
            fmt::print(stderr, "   {}|", depth);
    }

    virtual void print(int depth) const
    {
        print_depth(depth);
        fmt::print(stderr, "object type = {} name = {}\n", type, name);

        for (auto* child : children)
            child->print(depth + 1);
    }

    virtual void fill_token(H5O_token_t& token)
    {
        for (size_t i = 0; i < H5O_MAX_TOKEN_SIZE; ++i)
            token.__data[i] = 0;

        memcpy(token.__data, &this->token, sizeof(std::uintptr_t));

        // debug
        //fmt::print(stderr, "fill_token: {} ", fmt::ptr(this));
        //print_token(token);
    }

    // for debugging
    void print_token(H5O_token_t& token)
    {
        for (size_t i = 0; i < H5O_MAX_TOKEN_SIZE; ++i)
            fmt::print(stderr, "{0:x}", token.__data[i]);
        fmt::print(stderr, "\n");
    }

    Object* find_token(std::uintptr_t t)
    {
        if (token == t)
            return this;

        for (auto* child : children)
        {
            auto* res = child->find_token(t);
            if (res)
                return res;
        }

        return nullptr;
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
        else if (loc_params.type == H5VL_OBJECT_BY_IDX)
        {
            auto log = get_logger();
            log->warn("locate by index (H5VL_OBJECT_BY_IDX) is alpha quality");
            auto n = loc_params.loc_data.loc_by_idx.n;
            return ObjectPath { children[n], "" };
        }
        else if (loc_params.type == H5VL_OBJECT_BY_TOKEN)
        {
            H5O_token_t* token = loc_params.loc_data.loc_by_token.token;
            std::uintptr_t t;
            memcpy(&t, token->__data, sizeof(std::uintptr_t));

            auto log = get_logger();
            log->trace("locating by token: {}", t);

            Object* o = find_root()->find_token(t);
//            assert(o);

            if (o)
                log->trace("located by token {}: {} name = {}", t, fmt::ptr(o), o->name);
            else
                log->trace("NOT FOUND by token");

            return ObjectPath { o, "" };
        }
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

        if (path == ".")
            return ObjectPath { this, "" };

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
            full_path = ((!o->name.empty() && o->name[0] != '/')? "/" : "") + o->name + full_path;
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
    }

    void move_children(Object* from)
    {
        children = std::move(from->children);
        for (auto* c : children)
            c->parent = this;
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
