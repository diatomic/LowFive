#ifndef LOWFIVE_VOL_METADATA_HPP
#define LOWFIVE_VOL_METADATA_HPP

#include    <vector>
#include    <string>

#include    <fmt/core.h>
#include    "vol-base.hpp"
#include    "metadata.hpp"

namespace LowFive
{

struct LocationPattern
{
    std::string         filename;
    std::string         pattern;
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
    using LocationPatterns  = std::vector<LocationPattern>;

    struct ObjectPointers
    {
        void*           h5_obj;             // HDF5 object (e.g., dset)
        void*           mdata_obj;          // metadata object (tree node)

        ObjectPointers() : h5_obj(nullptr), mdata_obj(nullptr)
                    {}
    };

    Files                       files;
    LocationPatterns            memory;
    LocationPatterns            passthru;
    LocationPatterns            zerocopy;

                    MetadataVOL()
                    {}

                    ~MetadataVOL()
    {
        for (auto& x : files)
            delete x.second;
    }

    void*           wrap(void* p) override
    {
        ObjectPointers* op = new ObjectPointers;
        op->h5_obj = p;
        return op;
    }
    void*           unwrap(void* p) override
    {
        ObjectPointers* op = static_cast<ObjectPointers*>(p);
        return op->h5_obj;
    }
    void            drop(void* p) override
    {
        ObjectPointers* op = static_cast<ObjectPointers*>(p);
        delete op;
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
    void set_zerocopy(std::string filename, std::string pattern)
    {
        zerocopy.emplace_back(LocationPattern { filename, pattern });
    }

    void set_passthru(std::string filename, std::string pattern)
    {
        passthru.emplace_back(LocationPattern { filename, pattern });
    }

    void set_memory(std::string filename, std::string pattern)
    {
        memory.emplace_back(LocationPattern { filename, pattern });
    }

    // ref: https://www.geeksforgeeks.org/wildcard-character-matching/
    // checks if two given strings; the first string may contain wildcard characters
    static bool match(const char *first, const char * second, bool partial = false)
    {
        if (*first == '\0' && *second == '\0')
            return true;

        if (*first == '*' && *(first+1) != '\0' && *second == '\0')
            return partial;

        if (*first == '?' || *first == *second)
            return match(first+1, second+1);

        if (*first == '*')
            return match(first+1, second) || match(first, second+1);

        return partial;
    }

    bool match_any(std::pair<std::string,std::string> filepath, const LocationPatterns& patterns, bool partial = false) const
    {
        return match_any(filepath.first, filepath.second, patterns, partial);
    }

    bool match_any(const std::string& filename, const std::string& full_path, const LocationPatterns& patterns, bool partial = false) const
    {
        return find_match(filename, full_path, patterns, partial) != -1;
    }

    int find_match(const std::string& filename, const std::string& full_path, const LocationPatterns& patterns, bool partial = false) const
    {
        for (int i = 0; i < patterns.size(); ++i)
        {
            auto& x = patterns[i];
            if (x.filename != filename) continue;
            if (match(x.pattern.c_str(), full_path.c_str(), partial))
                return i;
        }
        return -1;
    }

    void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;
    void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) override;

    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;

    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    void*           group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    herr_t          group_close(void *grp, hid_t dxpl_id, void **req) override;

    void*           attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    void*           attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    //herr_t          attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;
    //herr_t          attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_close(void *attr, hid_t dxpl_id, void **req) override;
};

}

#endif
