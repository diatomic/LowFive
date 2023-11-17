#pragma once

namespace LowFive
{

struct Dataset : public Object
{
    struct DataTriple
    {
        Datatype    type;       // memory type
        Dataspace   memory;
        Dataspace   file;
        const void* data;
        std::unique_ptr<char[]> owned_data;
    };

    enum Ownership
    {
        user,                   // lowfive has only shallow pointer
        lowfive                 // lowfive deep copies data
    };

    using DataTriples = std::vector<DataTriple>;

    Datatype                        type;
    Dataspace                       space;
    DataTriples                     data;
    Ownership                       ownership;
    Hid                             dcpl;                   // hdf5 id of dataset creation property list
    Hid                             dapl;                   // hdf5 id of dataset access property list
    bool                            is_passthru {false };   // true if dataset that should go to disk
    bool                            is_memory {false };     // true if dataset should be kept in memory

    std::vector<std::string>        strings;

    Dataset(std::string name, hid_t dtype_id, hid_t space_id, Ownership own, Hid dcpl_, Hid dapl_, bool is_passthru_, bool is_memory_):
            Object(ObjectType::Dataset, name), type(dtype_id), space(space_id), ownership(own), dcpl(dcpl_), dapl(dapl_),
            is_passthru(is_passthru_),
            is_memory(is_memory_)
    {
        if (type.is_var_length_string())
            own = Ownership::lowfive;     // always take ownership of strings, otherwise serialization becomes tricky
    }

    void write(Datatype type, Dataspace memory, Dataspace file, const void* buf)
    {
        // make copies of the file and memory dataspaces here to keep their boxes as they were at this moment
        Dataspace& memory_ds_1 = (memory.id != H5S_ALL ? memory : space);
        Dataspace memory_ds = Dataspace(memory_ds_1.copy(), true);

        Dataspace& file_ds_1 = (file.id != H5S_ALL ? file : space);
        Dataspace file_ds = Dataspace(file_ds_1.copy(), true);

        if (ownership == Ownership::lowfive)
        {
            if (type.is_var_length_string())
            {
                size_t sz = memory_ds.size();

                // NB: we are going to store indices into strings, to make
                // serialization possible, using pointer-size type, to make any
                // calculations based on Dataspaces align with char* that would
                // normally be stored here
                intptr_t* p = new intptr_t[sz];
                for (size_t i = 0; i < sz; ++i)
                {
                    strings.push_back(std::string(((const char**) buf)[i]));
                    p[i] = strings.size() - 1;
                }
                data.emplace_back(DataTriple { type, memory_ds, file_ds, p, std::unique_ptr<char[]>((char*) (void*) p) });
            } else
            {
                size_t nbytes   = memory_ds.size() * type.dtype_size;
                char* p         = new char[nbytes];
                std::memcpy(p, buf, nbytes);
                data.emplace_back(DataTriple { type, memory_ds, file_ds, p, std::unique_ptr<char[]>(p) });
            }
        }
        else
            data.emplace_back(DataTriple { type, memory_ds, file_ds, buf, std::unique_ptr<char[]>(nullptr) });
    }

    void read(hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, void* buf)
    {
        for (auto& dt : data)                               // for all the data triples in the metadata dataset
        {
            Dataspace& fs = dt.file;

            hid_t file_space_id_1 = (file_space_id == H5S_ALL) ? fs.id : file_space_id;
            hid_t mem_space_id_1  = (mem_space_id == H5S_ALL)  ? fs.id : mem_space_id;
            hid_t mem_dst = Dataspace::project_intersection(file_space_id_1, mem_space_id_1, fs.id);
            hid_t mem_src = Dataspace::project_intersection(fs.id,           dt.memory.id,   file_space_id_1);

            Dataspace dst(mem_dst, true);
            Dataspace src(mem_src, true);

            if (type.is_var_length_string())
            {
                Dataspace::iterate(dst, Datatype(mem_type_id).dtype_size, src, type.dtype_size, [&](size_t loc1, size_t loc2, size_t len)
                {
                  char** to = (char**) ((char*) buf + loc1);
                  intptr_t* from = (intptr_t*) ((char*) dt.data + loc2);
                  for (size_t i = 0; i < len / sizeof(intptr_t); ++i)
                  {
                    auto idx = from[i];
                    if (!strings[idx].empty())
                    {
                        // presumably HDF5 will manage this string and reclaim the
                        // memory; I'm unclear on how this works, so it's a potential
                        // memory leak
                        char *cstr = new char[strings[idx].length() + 1];
                        strcpy(cstr, strings[idx].c_str());
                        to[i] = cstr;
                    } else
                        to[i] = NULL;
                  }
                });
            } else
            {
                Dataspace::iterate(dst, Datatype(mem_type_id).dtype_size, src, type.dtype_size, [&](size_t loc1, size_t loc2, size_t len)
                {
                  std::memcpy((char*) buf + loc1, (char*) dt.data + loc2, len);
                });
            }
        }   // for all data triples
    }

    void print(int depth) const override
    {
        print_depth(depth);
        fmt::print(stderr, "---- Dataset ---\n");

        print_depth(depth);
        fmt::print(stderr, "type = {}, space = {}, ownership = {}\n", type, space, ownership);

        if (data.size())
        {
            print_depth(depth);
            for (auto& d : data)
                fmt::print(stderr, "memory = {}, file = {}, data = {}\n", d.memory, d.file, fmt::ptr(d.data));
        }

        Object::print(depth);
    }
};

}
