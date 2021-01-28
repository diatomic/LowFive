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
        type(type_)
    {
        // remove path from name
        std::size_t found = name_.find_last_of("/");
        while (found == name_.size() - 1)          // remove any trailing slashes
        {
            name_ = name_.substr(0, found);
            found = name_.find_last_of("/");
        }
        name = name_.substr(found + 1);
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

    virtual Object* search(std::string& cur_path)                       // current path to the name with objects already found removed, will be truncated further
    {
        // remove any trailing slashes from current path
        std::size_t found = cur_path.find_last_of("/");
        while (found == cur_path.size() - 1)
            cur_path = cur_path.substr(0, found);

        size_t start    = cur_path.find_first_not_of("/");              // starting position of name in path
        size_t end      = cur_path.find_first_of("/", start);           // ending position of name in path

        if (start == std::string::npos)                                 // no name
            return NULL;
        else if (end == std::string::npos)                              // current path is just the name
        {
            if (name == cur_path)
                return this;
            return NULL;
        }
        else                                                            // peel the name from the front and update current path
        {
            std::string obj_name = cur_path.substr(start, end - start); // name peeled from front of path
            if (name != obj_name)
                return NULL;
            cur_path = cur_path.substr(end + 1);
            Object* obj;
            for (auto* child : children)                                // recurse subtree
            {
                if ((obj = child->search(cur_path)) != NULL)
                    return obj;
            }
            return NULL;
        }
        return nullptr;
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
