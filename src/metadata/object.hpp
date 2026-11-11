#pragma once

#include <algorithm>
#include <unordered_set>
#include "objecttype.hpp"
#include "error.hpp"
#include "../log-private.hpp"

namespace LowFive
{

struct Object
{
    Object*                         parent;
    Object*                         root;
    std::vector<Object*>            children;
    std::uintptr_t                  token;
    H5O_token_t                     connector_token {};
    bool                            has_connector_token = false;

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
            if (!obj || !path.empty())
                throw MetadataError(fmt::format("Expected to find exact object, but got remainder: {}", path));
            return obj;
        }
    };

    Object(ObjectType type_, std::string name_):
        parent(nullptr),
        root(this),
        type(type_),
        name(std::move(name_))
    {
        token = reinterpret_cast<std::uintptr_t>(this);
    }

    virtual ~Object()
    {
        remove();

        // Links reference targets without owning them; children form the owning tree.
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

    virtual void fill_token(H5O_token_t& output)
    {
        if (has_connector_token)
        {
            output = connector_token;
            return;
        }
        std::memset(output.__data, 0, H5O_MAX_TOKEN_SIZE);
        std::memcpy(output.__data, &token, sizeof(token));
    }

    // for debugging
    void print_token(H5O_token_t& token)
    {
        for (size_t i = 0; i < H5O_MAX_TOKEN_SIZE; ++i)
            fmt::print(stderr, "{0:x}", token.__data[i]);
        fmt::print(stderr, "\n");
    }

    virtual Object* find_token(std::uintptr_t t)
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
            const auto& by = loc_params.loc_data.loc_by_idx;
            auto group = search(by.name).exact();
            if (!group) return {nullptr, ""};
            auto entries = group->links(by.idx_type, by.order);
            if (by.n >= entries.size()) return {nullptr, ""};
            unsigned budget = 64;
            return {entries[by.n]->resolve_link(budget), ""};
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

    std::vector<Object*> links(H5_index_t index = H5_INDEX_NAME, H5_iter_order_t order = H5_ITER_INC)
    {
        std::vector<Object*> result;
        for (auto* child : children)
            if (child->type != ObjectType::Attribute) result.push_back(child);
        if (index == H5_INDEX_NAME)
            std::sort(result.begin(), result.end(), [](Object* a, Object* b) { return a->name < b->name; });
        else if (index != H5_INDEX_CRT_ORDER)
            throw MetadataError("Unsupported link index");
        if (order == H5_ITER_DEC) std::reverse(result.begin(), result.end());
        return result;
    }

    virtual Object* resolve_link(unsigned&) { return this; }
    virtual Object* link_target() { return nullptr; }

    bool find_reachable_path(Object* target, const std::string& prefix,
                             std::unordered_set<Object*>& active,
                             std::string& result)
    {
        if (this == target)
        {
            result = prefix.empty() ? "/" : prefix;
            return true;
        }
        if (!active.insert(this).second) return false;
        for (auto* child : children)
        {
            if (child->type == ObjectType::Attribute) continue;
            auto* next = child->link_target();
            if (!next) next = child;
            const auto path = prefix + "/" + child->name;
            if (next->find_reachable_path(target, path, active, result))
                return true;
        }
        active.erase(this);
        return false;
    }

    std::string reachable_path(Object* target)
    {
        std::unordered_set<Object*> active;
        std::string result;
        find_reachable_path(target, "", active, result);
        return result;
    }

    ObjectPath search_path(std::string path, unsigned& budget, bool follow_final = true)
    {
        Object* current = this;
        if (!path.empty() && path.front() == '/') current = find_root();
        size_t offset = 0;
        while (offset < path.size())
        {
            while (offset < path.size() && path[offset] == '/') ++offset;
            if (offset == path.size()) break;
            auto end = path.find('/', offset);
            auto component = path.substr(offset, end == std::string::npos ? end : end - offset);
            if (component != ".")
            {
                Object* child = nullptr;
                for (auto* candidate : current->children)
                    if (candidate->type != ObjectType::Attribute && candidate->name == component) { child = candidate; break; }
                if (!child) return {current, path.substr(offset)};
                bool final = end == std::string::npos || path.find_first_not_of('/', end) == std::string::npos;
                current = (follow_final || !final) ? child->resolve_link(budget) : child;
                if (!current) return {nullptr, path.substr(offset)};
            }
            if (end == std::string::npos) break;
            offset = end + 1;
        }
        return {current, ""};
    }

    virtual ObjectPath search(std::string path)
    {
        unsigned budget = 64;
        return search_path(std::move(path), budget);
    }

    ObjectPath locate_link(const H5VL_loc_params_t& loc)
    {
        unsigned budget = 64;
        if (loc.type == H5VL_OBJECT_BY_NAME)
            return search_path(loc.loc_data.loc_by_name.name, budget, false);
        if (loc.type == H5VL_OBJECT_BY_IDX)
        {
            const auto& by = loc.loc_data.loc_by_idx;
            auto group = search(by.name).exact();
            if (!group) return {nullptr, ""};
            auto entries = group->links(by.idx_type, by.order);
            return {by.n < entries.size() ? entries[by.n] : nullptr, ""};
        }
        return locate(loc);
    }

    Object* attribute(const std::string& attr_name)
    {
        for (auto* child : children)
            if (child->type == ObjectType::Attribute && child->name == attr_name) return child;
        return nullptr;
    }

    Object* add_path(Object* leaf, const std::string& path)
    {
        auto missing = search(path);
        Object* owner = missing.obj;
        auto remainder = missing.path;
        if (!owner || remainder.empty()) { delete leaf; return nullptr; }
        for (auto slash = remainder.find('/'); slash != std::string::npos; slash = remainder.find('/'))
        {
            auto component = remainder.substr(0, slash);
            if (!component.empty() && component != ".")
                owner = owner->add_child(new Object(ObjectType::Group, component));
            remainder.erase(0, slash + 1);
        }
        leaf->name = remainder;
        return owner->add_child(leaf);
    }

    std::pair<std::string, std::string> fullname(std::string child_path = "")     // returns filename, full path pair
    {
        Object* file = find_root();
        if (!child_path.empty() && child_path.front() == '/')
            return {file->name, child_path};
        std::string full_path;
        for (Object* object = this; object != file; object = object->parent)
        {
            if (!object->parent)
            {
                auto reachable = file->reachable_path(object);
                if (reachable.empty()) return {file->name, ""};
                full_path = reachable + full_path;
                break;
            }
            full_path = "/" + object->name + full_path;
        }
        if (!child_path.empty()) full_path += "/" + child_path;
        if (full_path.empty()) full_path = "/";
        return {file->name, full_path};
    }

    void set_root(Object* new_root)
    {
        root = new_root;
        for (auto* child : children) child->set_root(new_root);
    }

    Object* add_child(Object* object)
    {
        children.push_back(object);
        object->parent = this;
        object->set_root(find_root());
        return object;
    }

    void move_children(Object* from)
    {
        children = std::move(from->children);
        for (auto* child : children)
        {
            child->parent = this;
            child->set_root(find_root());
        }
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
        return root;
    }
};

}
