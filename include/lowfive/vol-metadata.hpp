#ifndef LOWFIVE_VOL_METADATA_HPP
#define LOWFIVE_VOL_METADATA_HPP

#include    <fmt/core.h>
#include    "vol-base.hpp"
#include    "metadata.hpp"
#include    <diy/mpi.hpp>

namespace LowFive
{

// properties of the entire VOL object
// can be overwritten by individual objects
struct VOLProperties
{
    bool memory;        // in-memory copy (coul be deep or shallow) of all files
    bool passthru;      // pass-through to native HDF5 for all files
    bool copy;          // make a deep copy if memory == true
};

// TODO: eventually add more custom properties only for files
// or move some of the VOL properties to the file level
// TODO: move to metadata/file.hpp?
struct FileProperties: public VOLProperties
{
};

// TODO: eventually add more custom properties only for datasets
// or move some of the file properties to the dataset level
// TODO: move to metadata/dataset.hpp?
struct DatasetProperties: public FileProperties
{
};

// dataset ownership (relevant for producer only)
struct DataOwnership
{
    std::string         filename;
    std::string         full_path;
    Dataset::Ownership  ownership;
};

// custom VOL object
// only need to specialize those functions that are custom

struct MetadataVOL: public LowFive::VOLBase
{
    using VOLBase::VOLBase;

    using File              = LowFive::File;
    using Object            = LowFive::Object;
    using Dataset           = LowFive::Dataset;
    using Dataspace         = LowFive::Dataspace;
    using Group             = LowFive::Group;

    using Files             = std::map<std::string, File*>;
    using DataOwners        = std::vector<DataOwnership>;

    VOLProperties   vol_properties;

    struct ObjectPointers
    {
        void*           h5_obj;             // HDF5 object (e.g., dset)
        void*           mdata_obj;          // metadata object (tree node)

        ObjectPointers() : h5_obj(nullptr), mdata_obj(nullptr)
                    {}
    };

    Files           files;
    DataOwners      data_owners;

                    MetadataVOL():
                        VOLBase(/* version = */ 0, /* value = */ 510, /* name = */ "metadata-vol")
                    {}

                    ~MetadataVOL()
    {
        for (auto& x : files)
            delete x.second;
    }

    void            drop(std::string filename)
    {
        auto it = files.find(filename);
        delete it->second;
        files.erase(it);
    }

    void            print_files()
    {
        for (auto& f : files)
        {
            fmt::print("--------------------------------------------------------------------------------\n");
            fmt::print("File {}\n", f.first);
            f.second->print();
            fmt::print("--------------------------------------------------------------------------------\n");
        }
    }

    // locate an object in the metadata of one file by its full path, which uniquely identifies one object
    Object*         locate(std::string filename, std::string full_path) const
    {
        Object* obj;
        auto it = files.find(filename);
        if (it == files.end())
            return NULL;
        return it->second->search(full_path);
    }

    // record intended ownership of a dataset
    void data_ownership(std::string filename, std::string full_path, Dataset::Ownership own)
    {
        data_owners.emplace_back(DataOwnership { filename, full_path, own });
    }

    // trace object back to root to build full path and file name
    static
    void backtrack_name(
            std::string     name,                   // name of current object
            Object*         parent,                 // parent of current object
            std::string&    filename,               // (output) file name of enclosing file
            std::string&    full_path)              // (output) full path name of object in file
    {
        std::tie(filename,full_path) = parent->fullname();
        full_path += "/" + name;
    }

    // ref: https://www.geeksforgeeks.org/wildcard-character-matching/
    // checks if two given strings; the first string may contain wildcard characters
    bool match(const char *first, const char * second)
    {
        if (*first == '\0' && *second == '\0')
            return true;

        if (*first == '*' && *(first+1) != '\0' && *second == '\0')
            return false;

        if (*first == '?' || *first == *second)
            return match(first+1, second+1);

        if (*first == '*')
            return match(first+1, second) || match(first, second+1);

        return false;
    }

    void*           info_copy(const void *_info) override;

    void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;
    void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) override;

    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;

    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    herr_t          group_close(void *grp, hid_t dxpl_id, void **req) override;

    void*           attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    //void*           attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    //herr_t          attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    //herr_t          attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;
    //herr_t          attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_close(void *attr, hid_t dxpl_id, void **req) override;
};

}

#endif
