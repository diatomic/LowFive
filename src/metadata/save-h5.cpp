#include <lowfive/metadata.hpp>
#include "../log-private.hpp"

namespace LowFive
{

void save_dataset_data(const Hid& dataset, const Dataset* d)
{
    for (auto& x : d->data)
    {
        if (x.file.selection != Dataspace::Selection::hyperslabs && x.file.selection != Dataspace::Selection::all)
            throw MetadataError(fmt::format("encountered unexpected non-hyperslab selection: {}", x.file.selection));

        H5Dwrite(dataset.id, x.type.id, x.memory.id, x.file.id, H5P_DEFAULT, x.data);
    }
}

void save_children(const Hid& x, const Object* p)
{
    for (const Object* o : p->children)
    {
        if (const Group* g = dynamic_cast<const Group*>(o))
        {
            Hid group = H5Gcreate(x.id, g->name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
            save_children(group, g);
            H5Gclose(group.id);
        } else if (const Dataset* d = dynamic_cast<const Dataset*>(o))
        {
            // create dataset
            Hid dataset = H5Dcreate(x.id, d->name.c_str(), d->type.id, d->space.id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
            save_dataset_data(dataset, d);
            H5Dclose(dataset.id);
        } else if (const Attribute* a = dynamic_cast<const Attribute*>(o))
        {
            // create dataset
            Hid attribute = H5Acreate(x.id, a->name.c_str(), a->type.id, a->space.id, H5P_DEFAULT, H5P_DEFAULT);
            H5Awrite(attribute.id, a->mem_type.id, a->data.get());
            H5Aclose(attribute.id);
        }
    }
}

}

void
LowFive::save(const File& f, std::string filename)
{
    auto log = LowFive::get_logger();

    log->trace("Saving {} to {}", f.name, filename);

    Hid fid = H5Fcreate(filename.c_str(),
                       H5F_ACC_TRUNC,
                       H5P_DEFAULT,
                       H5P_DEFAULT);

    save_children(fid, &f);

    H5Fclose(fid.id);
}
