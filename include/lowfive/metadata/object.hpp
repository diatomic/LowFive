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

    Object(ObjectType   type_,
           std::string  name_,
           bool         remove_path = true) :       // strip path from name
        parent(nullptr),
        type(type_)
    {
        // remove path from name
        if (remove_path)
        {
            std::size_t found = name_.find_last_of("/");
            while (found == name_.size() - 1)          // remove any trailing slashes
            {
                name_ = name_.substr(0, found);
                found = name_.find_last_of("/");
            }
            name = name_.substr(found + 1);
        }
        else
            name = name_;
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
    }

    virtual void print() const
    {
        fmt::print(stderr, "object type = {} name = {}\n", type, name);
        // TODO: print attributes and properties

        for (auto* child : children)
            child->print();
    }

    // search for the object at the leaf of the current path in the subtree rooted at this object
    // this object can be either the root of the current path or its direct parent
    // returns pointer to object or null if not found
    virtual Object* search(std::string& cur_path)                       // current path to the name with objects already found removed, will be truncated further
    {
        // remove any trailing slashes from current path
        std::size_t found = cur_path.find_last_of("/");
        while (found == cur_path.size() - 1)
            cur_path = cur_path.substr(0, found);

        size_t start, end;                                              // starting and ending position of root name in path
        if (cur_path[0] == '/')
        {
            start   = 0;
            end     = 1;
        }
        else
        {
            start = cur_path.find_first_not_of("/");
            end = cur_path.find_first_of("/", start);
        }

        if (start == std::string::npos)                                 // no name
            return nullptr;
        else if (end == std::string::npos)                              // current path is the leaf
        {
            if (name == cur_path)
                return this;
            return nullptr;
        }
        else                                                            // current path is not the leaf yet
        {
            std::string obj_name = cur_path.substr(start, end - start); // name peeled from front of path
            Object* parent = this;                                      // parent of subtree to search

            if (obj_name == "/" && type == ObjectType::File)            // root of curent path is the file (assumes file name is correct)
                cur_path = cur_path.substr(end);
            else if (obj_name == name)                                  // root of curent path is this object
                cur_path = cur_path.substr(end + 1);
            else                                                        // root of current path could be one of the direct children
            {
                bool found_child = false;
                for (auto* child : children)
                {
                    if (child->name == obj_name)
                    {
                        cur_path = cur_path.substr(end + 1);
                        found_child = true;
                        parent = child;
                        break;
                    }
                }
                if (!found_child)
                    return nullptr;
            }

            // recurse subtree
            Object* obj;
            for (auto* child : parent->children)
            {
                if ((obj = child->search(cur_path)) != NULL)
                    return obj;
            }
            return nullptr;
        }
        return nullptr;
    }

    std::pair<std::string, std::string> fullname()     // returns filename, full path pair
    {
        std::string full_path;

        Object* o = this;

        while (o->type != ObjectType::File)
        {
            full_path = "/" + o->name + full_path;
            o = o->parent;
        }

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
};

}
