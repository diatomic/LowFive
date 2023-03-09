#ifndef LOWFIVE_VOL_METADATA_HPP
#define LOWFIVE_VOL_METADATA_HPP

#include    <vector>
#include    <string>
#include    <map>
#include    <unordered_set>
#include    <functional>

#include    <fmt/core.h>
#include    "vol-base.hpp"

namespace LowFive
{

struct LocationPattern
{
    std::string         filename;
    std::string         pattern;
};

struct Object;              // forward declaration for Files
struct ObjectPointers;      // forward declaration for wrap

// custom VOL object
// only need to specialize those functions that are custom

struct MetadataVOL: public LowFive::VOLBase
{
    using VOLBase::VOLBase;

    using Files             = std::map<std::string, Object*>;       // Object*, to support both File and RemoteFile
    using LocationPatterns  = std::vector<LocationPattern>;

    using AfterFileClose    = std::function<void()>;
    using BeforeFileOpen    = std::function<void()>;
    using AfterDatasetWrite = std::function<void()>;

    Files                       files;
    LocationPatterns            memory;
    LocationPatterns            passthru;
    LocationPatterns            zerocopy;
    bool                        keep = false;       // whether to keep files in the metadata after they are closed

    // callbacks
    AfterFileClose              after_file_close;
    BeforeFileOpen              before_file_open;
    AfterDatasetWrite           after_dataset_write;

    protected:
                    MetadataVOL()
                    {}
    public:

                    ~MetadataVOL() override;

                    // prohibit copy and move
                    MetadataVOL(const MetadataVOL&)=delete;
                    MetadataVOL(MetadataVOL&&)=delete;

                    MetadataVOL& operator=(const MetadataVOL&)=delete;
                    MetadataVOL& operator=(MetadataVOL&&)=delete;

    static MetadataVOL&         create_MetadataVOL();

    //bool dont_wrap = false;
    std::unordered_set<void*>   our_objects;
    bool            ours(void* p) const     { return our_objects.find(p) != our_objects.end(); }

    ObjectPointers* wrap(void* p);
    void*           unwrap(void* p);
    void            drop(void* p) override;

    void            drop(std::string filename);

    void            print_files();

    // locate an object in the metadata of one file by its full path, which uniquely identifies one object
    Object*         locate(std::string filename, std::string full_path) const;

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

    void set_keep(bool keep_)
    {
        keep = keep_;
    }

    void set_after_file_close(AfterFileClose afc)
    {
        after_file_close = afc;
    }

    void set_before_file_open(BeforeFileOpen bfo)
    {
        before_file_open = bfo;
    }

    void set_after_dataset_write(AfterDatasetWrite adw)
    {
        after_dataset_write = adw;
    }

    void clear_files();

    // ref: https://www.geeksforgeeks.org/wildcard-character-matching/
    // checks if two given strings; the first string may contain wildcard characters
    static bool match(const char *first, const char * second, bool partial = false)
    {
        if (*first == '\0' && *second == '\0')
            return true;

        if (*first == '*' && *(first+1) != '\0' && *second == '\0')
            return partial;

        if (*first == '?' || *first == *second)
            return match(first+1, second+1, partial);

        if (*first == '*')
            return match(first+1, second, partial) || match(first, second+1, partial);

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
        for (int i = 0; i < static_cast<int>(patterns.size()); ++i)
        {
            auto& x = patterns[i];
            if (!match(x.filename.c_str(), filename.c_str())) continue;
            if (match(x.pattern.c_str(), full_path.c_str(), partial))
                return i;
        }
        return -1;
    }

    std::vector<int>
        find_matches(const std::string& filename, const std::string& full_path, const LocationPatterns& patterns, bool partial = false) const
    {
        std::vector<int> result;
        for (int i = 0; i < static_cast<int>(patterns.size()); ++i)
        {
            auto& x = patterns[i];
            if (!match(x.filename.c_str(), filename.c_str())) continue;
            if (match(x.pattern.c_str(), full_path.c_str(), partial))
                result.push_back(i);
        }
        return result;
    }

    void*           file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_optional(void *file, H5VL_file_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          file_close(void *file, hid_t dxpl_id, void **req) override;
    void*           file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) override;
    herr_t          file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;

    void*           dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req) override;
    herr_t          dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req) override;
    void*           dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req) override;
    herr_t          dataset_close(void *dset, hid_t dxpl_id, void **req) override;
    herr_t          dataset_specific(void *obj, H5VL_dataset_specific_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          dataset_optional(void *obj, H5VL_dataset_optional_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;

    void*           group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    void*           group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req) override;
    herr_t          group_close(void *grp, hid_t dxpl_id, void **req) override;
    herr_t          group_optional(void *obj, H5VL_group_optional_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          group_get(void *dset, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          group_specific(void *obj, H5VL_group_specific_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;

    herr_t          link_create(H5VL_link_create_type_t create_type, void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req) override;
    herr_t          link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req) override;
    herr_t          link_get(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, H5VL_link_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          link_specific(void *obj, const H5VL_loc_params_t *loc_params, hid_t under_vol_id, H5VL_link_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          link_optional(void *obj, hid_t under_vol_id, H5VL_link_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          link_iter(void *obj, va_list arguments);


    herr_t          link_create_trampoline(H5VL_link_create_type_t create_type, void *obj,
                                           const H5VL_loc_params_t *loc_params, hid_t under_vol_id, hid_t lcpl_id, hid_t lapl_id,
                                           hid_t dxpl_id, void **req, ...);

    void*           attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    void*           attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req) override;
    herr_t          attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req) override;
    herr_t          attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_optional(void *obj, H5VL_attr_optional_t opt_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          attr_close(void *attr, hid_t dxpl_id, void **req) override;
    htri_t          attr_exists(Object *mdata_obj, va_list arguments);
    herr_t          attr_iter(void *obj, va_list arguments);

    void *          object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req) override;
    herr_t          object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req) override;
    herr_t          object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) override;
    herr_t          object_optional(void *obj, int op_type, hid_t dxpl_id, void **req, va_list arguments) override;

    herr_t          introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls) override;
    herr_t          introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, hbool_t *supported) override;

    herr_t          blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx) override;
    herr_t          blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx) override;
    herr_t          blob_specific(void *obj, void *blob_id, H5VL_blob_specific_t specific_type, va_list arguments) override;

    herr_t          token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value) override;

    void*           wrap_get_object(void *obj) override;
    herr_t          get_wrap_ctx(void *obj, void **wrap_ctx) override;
    void*           wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx) override;
    void*           unwrap_object(void *obj) override;
    //herr_t          free_wrap_ctx(void *obj) override;
};

}

#endif
