#include "../vol-metadata-private.hpp"

void
LowFive::serialize(diy::BinaryBuffer& bb, Object* o)
{
    diy::save(bb, o->type);
    diy::save(bb, o->name);
    diy::save(bb, o->children.size());

    if (o->type == ObjectType::Dataset)
    {
        auto* d = static_cast<Dataset*>(o);
        diy::save(bb, d->type);
        diy::save(bb, d->space);
        diy::save(bb, d->ownership);
    }
    else if (o->type == ObjectType::Attribute)
    {
        auto* a = static_cast<Attribute*>(o);
        diy::save(bb, a->type);
        diy::save(bb, a->space);
        diy::save(bb, a->mem_type);
        if (!a->mem_type.is_var_length_string())
            diy::save(bb, a->data.get(), a->mem_type.dtype_size);
        else
            diy::save(bb, a->strings);
    }
    else if (o->type == ObjectType::HardLink)
        throw MetadataError("cannot serialize hard links");
    else if (o->type == ObjectType::SoftLink)
        diy::save(bb, static_cast<SoftLink*>(o)->target);

    for (Object* child : o->children)
        serialize(bb, child);
}

LowFive::Object*
LowFive::deserialize(diy::BinaryBuffer& bb)
{
    ObjectType type;
    std::string name;
    size_t n_children;

    diy::load(bb, type);
    diy::load(bb, name);
    diy::load(bb, n_children);

    // figure out the type
    Object* o;
    if (type == ObjectType::File)
        o = new File(name, H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
    else if (type == ObjectType::Group)
        o = new Group(name, H5P_GROUP_CREATE_DEFAULT);
    else if (type == ObjectType::Dataset)
    {
        Datatype dt;
        Dataspace s;
        Dataset::Ownership own;

        diy::load(bb, dt);
        diy::load(bb, s);
        diy::load(bb, own);

        o = new Dataset(name, dt.id, s.id, own, H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT);
    }
    else if (type == ObjectType::Attribute)
    {
        Datatype dt;
        Dataspace s;

        diy::load(bb, dt);
        diy::load(bb, s);

        auto* a = new Attribute(name, dt.id, s.id);
        diy::load(bb, a->mem_type);
        if (!a->mem_type.is_var_length_string())
        {
            a->data = std::unique_ptr<char>(new char[a->mem_type.dtype_size]);
            diy::load(bb, a->data.get(), a->mem_type.dtype_size);
        } else
            diy::load(bb, a->strings);

        o = a;
    }
    else if (type == ObjectType::NamedDtype)
        o = new NamedDtype(name);
    else if (type == ObjectType::SoftLink)
        throw MetadataError("cannot deserialize hard links");
    else if (type == ObjectType::HardLink)
    {
        std::string target;
        diy::load(bb, target);
        o = new SoftLink(name, target);
    } else
        MetadataError("unhandled case in deserialization");

    for (size_t i = 0; i < n_children; ++i)
        o->add_child(deserialize(bb));

    return o;
}
