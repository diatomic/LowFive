#include "test.hpp"
#include "../src/metadata.hpp"
#include "../src/vol-metadata-private.hpp"
#include <array>
#include <cstring>
#include <vector>

namespace
{
using test::Id;
Id dataset(hid_t parent, const char* name, hid_t space)
{
    return Id(H5Dcreate2(parent, name, H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
}
void write(hid_t d, int value) { CHECK(H5Dwrite(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value) >= 0); }
void value(hid_t d, int expected) { int actual = -1; CHECK(H5Dread(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &actual) >= 0); CHECK(actual == expected); }
herr_t collect_attribute(hid_t, const char* name, const H5A_info_t*, void* data)
{
    static_cast<std::vector<std::string>*>(data)->emplace_back(name);
    return 0;
}
void native_callbacks(const std::string& name)
{
    const std::string filename = "callbacks_" + name + ".h5";
    const std::string second_filename = filename + ".second";
    auto scalar = test::scalar();
    {
        Id file(H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
        if (name == "native_object_dataset_extent")
        {
            hsize_t dimensions = 1;
            hsize_t maximum = 4;
            Id space(H5Screate_simple(1, &dimensions, &maximum), H5Sclose);
            Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
            hsize_t chunk = 1;
            CHECK(H5Pset_chunk(dcpl, 1, &chunk) >= 0);
            Id values(H5Dcreate2(file, "values", H5T_NATIVE_INT, space,
                                  H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
        }
        else if (name == "native_link_delete_idx_uncached")
        {
            Id group(H5Gcreate2(file, "existing_group", H5P_DEFAULT,
                                H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            auto first = dataset(group, "a", scalar);
            auto second = dataset(group, "b", scalar);
            write(first, 1);
            write(second, 2);
        }
        else if (name == "native_link_delete_idx")
        {
            auto first = dataset(file, "a", scalar);
            auto second = dataset(file, "b", scalar);
            write(first, 1);
            write(second, 2);
        }
        else if (name == "native_datatype_attr" ||
                 name == "native_datatype_object_open" ||
                 name == "native_datatype_link_create")
        {
            int expected = 127;
            Id datatype(H5Tcopy(H5T_NATIVE_INT), H5Tclose);
            CHECK(H5Tcommit2(file, "native-type", datatype, H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT) >= 0);
            Id attribute(H5Acreate2(datatype, "label", H5T_NATIVE_INT, scalar,
                                    H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(attribute, H5T_NATIVE_INT, &expected) >= 0);
        }
        else if (name.rfind("native_attr_", 0) == 0)
        {
            int first_value = 11;
            int second_value = 22;
            Id first(H5Acreate2(file, "first", H5T_NATIVE_INT, scalar,
                                H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            Id second(H5Acreate2(file, "second", H5T_NATIVE_INT, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(first, H5T_NATIVE_INT, &first_value) >= 0);
            CHECK(H5Awrite(second, H5T_NATIVE_INT, &second_value) >= 0);
        }
        else
        {
            auto source = dataset(file, "source", scalar);
            write(source, 71);
            CHECK(H5Lcreate_soft("/source", file, "soft", H5P_DEFAULT, H5P_DEFAULT) >= 0);
            CHECK(H5Lcreate_soft("/source", file, "soft2", H5P_DEFAULT, H5P_DEFAULT) >= 0);
            Id group(H5Gcreate2(file, "group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            if (name == "native_group_soft_link_uncached" ||
                name == "native_dataset_intermediate_soft_link")
            {
                auto child = dataset(group, "child", scalar);
                write(child, 73);
            }
        }
    }
    if (name == "native_link_copy_move")
    {
        Id second(H5Fcreate(second_filename.c_str(), H5F_ACC_TRUNC,
                            H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
    }

    test::Connector connector("callbacks_" + name, "file");
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    Id file(H5Fopen(filename.c_str(), H5F_ACC_RDWR, connector.fapl), H5Fclose);
    if (name == "native_object_dataset_extent")
    {
        Id values(H5Oopen(file, "values", H5P_DEFAULT), H5Oclose);
        CHECK(values.id >= 0 && H5Iget_type(values) == H5I_DATASET);
        hsize_t extent = 2;
        CHECK(H5Dset_extent(values, &extent) >= 0);
        Id space(H5Dget_space(values), H5Sclose);
        hsize_t actual = 0;
        CHECK(H5Sget_simple_extent_dims(space, &actual, nullptr) == 1);
        CHECK(actual == extent);
    }
    else if (name == "native_datatype_attr")
    {
        Id datatype(H5Topen2(file, "native-type", H5P_DEFAULT), H5Tclose);
        Id attribute(H5Aopen(datatype, "label", H5P_DEFAULT), H5Aclose);
        int actual = -1;
        CHECK(H5Aread(attribute, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == 127);
        CHECK(H5Aexists(datatype, "label") > 0);
        CHECK(H5Arename(datatype, "label", "renamed") >= 0);
        CHECK(H5Aexists(datatype, "label") == 0);
        CHECK(H5Aexists(datatype, "renamed") > 0);
        CHECK(H5Adelete(datatype, "renamed") >= 0);
        CHECK(H5Aexists(datatype, "renamed") == 0);
    }
    else if (name == "native_datatype_object_open")
    {
        Id datatype(H5Topen2(file, "native-type", H5P_DEFAULT), H5Tclose);
        Id object(H5Oopen(datatype, ".", H5P_DEFAULT), H5Oclose);
        CHECK(object.id >= 0);
        CHECK(H5Iget_type(object) == H5I_DATATYPE);
    }
    else if (name == "native_datatype_link_create")
    {
        Id datatype(H5Topen2(file, "native-type", H5P_DEFAULT), H5Tclose);
        CHECK(H5Olink(datatype, file, "type-alias", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "type-alias", H5P_DEFAULT) > 0);
        Id alias(H5Topen2(file, "type-alias", H5P_DEFAULT), H5Tclose);
        CHECK(alias.id >= 0);
    }
    else if (name == "native_attr_iteration")
    {
        std::vector<std::string> names;
        hsize_t index = 0;
        CHECK(H5Aiterate2(file, H5_INDEX_NAME, H5_ITER_INC, &index,
                          collect_attribute, &names) >= 0);
        CHECK(names == std::vector<std::string>({"first", "second"}));
    }
    else if (name == "native_attr_open_delete")
    {
        Id attribute(H5Aopen(file, "first", H5P_DEFAULT), H5Aclose);
        CHECK(attribute.id >= 0);
        CHECK(H5Adelete(file, "first") >= 0);
        int actual = -1;
        CHECK(H5Aread(attribute, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == 11);
        attribute.reset();
        CHECK(H5Aexists(file, "first") == 0);
    }
    else if (name == "native_attr_delete_idx")
    {
        CHECK(H5Adelete_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0,
                               H5P_DEFAULT) >= 0);
        CHECK(H5Aexists(file, "first") == 0);
        CHECK(H5Aexists(file, "second") > 0);
    }
    else if (name == "native_attr_delete_idx_mixed")
    {
        { Id first(H5Aopen(file, "first", H5P_DEFAULT), H5Aclose); }
        { Id second(H5Aopen(file, "second", H5P_DEFAULT), H5Aclose); }
        vol.passthru.clear();
        vol.set_passthru(filename, "/first");
        vol.set_passthru(filename, "/second");
        int expected = 33;
        {
            Id memory(H5Acreate2(file, "memory", H5T_NATIVE_INT, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(memory, H5T_NATIVE_INT, &expected) >= 0);
        }
        CHECK(H5Adelete_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0,
                               H5P_DEFAULT) >= 0);
        CHECK(H5Aexists(file, "first") == 0);
        CHECK(H5Aexists(file, "second") > 0);
        Id memory(H5Aopen(file, "memory", H5P_DEFAULT), H5Aclose);
        int actual = -1;
        CHECK(H5Aread(memory, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == expected);
    }
    else if (name == "native_link_delete_idx")
    {
        Id second(H5Dopen2(file, "b", H5P_DEFAULT), H5Dclose);
        value(second, 2);
        vol.passthru.clear();
        vol.set_passthru(filename, "/a");
        vol.set_passthru(filename, "/b");
        int expected = 44;
        {
            Id memory(H5Acreate2(second, "memory", H5T_NATIVE_INT, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(memory, H5T_NATIVE_INT, &expected) >= 0);
        }
        second.reset();
        CHECK(H5Ldelete_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0,
                               H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "a", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(file, "b", H5P_DEFAULT) > 0);
        Id reopened(H5Dopen2(file, "b", H5P_DEFAULT), H5Dclose);
        Id memory(H5Aopen(reopened, "memory", H5P_DEFAULT), H5Aclose);
        int actual = -1;
        CHECK(H5Aread(memory, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == expected);
    }
    else if (name == "native_link_delete_idx_uncached")
    {
        CHECK(H5Ldelete_by_idx(file, "existing_group", H5_INDEX_NAME,
                               H5_ITER_INC, 0, H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "existing_group/a", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(file, "existing_group/b", H5P_DEFAULT) > 0);
    }
    else if (name == "native_link_move_cached")
    {
        { Id cached(H5Dopen2(file, "source", H5P_DEFAULT), H5Dclose);
          value(cached, 71); }
        CHECK(H5Lmove(file, "source", file, "moved", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "source", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(file, "moved", H5P_DEFAULT) > 0);
        auto recreated = dataset(file, "source", scalar);
        CHECK(recreated.id >= 0);
    }
    else if (name == "native_link_move_cycle")
    {
        Id group(H5Gopen2(file, "group", H5P_DEFAULT), H5Gclose);
        CHECK(group.id >= 0);
        CHECK(H5Lmove(file, "group", group, "self", H5P_DEFAULT,
                      H5P_DEFAULT) < 0);
        CHECK(H5Lexists(file, "group", H5P_DEFAULT) > 0);
        CHECK(H5Lexists(group, "self", H5P_DEFAULT) == 0);
    }
    else if (name == "native_soft_link_uncached")
    {
        CHECK(H5Lcreate_soft("/source", file, "middle", H5P_DEFAULT,
                            H5P_DEFAULT) >= 0);
        CHECK(H5Lcreate_soft("/middle", file, "alias", H5P_DEFAULT,
                            H5P_DEFAULT) >= 0);
        Id alias(H5Dopen2(file, "alias", H5P_DEFAULT), H5Dclose);
        value(alias, 71);
    }
    else if (name == "native_group_soft_link_uncached")
    {
        CHECK(H5Lcreate_soft("/group", file, "group-alias", H5P_DEFAULT,
                            H5P_DEFAULT) >= 0);
        Id alias(H5Gopen2(file, "group-alias", H5P_DEFAULT), H5Gclose);
        Id gcpl(H5Gget_create_plist(alias), H5Pclose);
        CHECK(gcpl.id >= 0);
        Id child(H5Dopen2(alias, "child", H5P_DEFAULT), H5Dclose);
        value(child, 73);
    }
    else if (name == "native_dataset_intermediate_soft_link")
    {
        CHECK(H5Lcreate_soft("/group", file, "group-alias", H5P_DEFAULT,
                            H5P_DEFAULT) >= 0);
        Id child(H5Dopen2(file, "group-alias/child", H5P_DEFAULT), H5Dclose);
        value(child, 73);
    }
    else if (name == "native_object_soft_link_uncached")
    {
        CHECK(H5Lcreate_soft("/source", file, "alias", H5P_DEFAULT,
                            H5P_DEFAULT) >= 0);
        Id alias(H5Oopen(file, "alias", H5P_DEFAULT), H5Oclose);
        CHECK(H5Iget_type(alias) == H5I_DATASET);
        value(alias, 71);
    }
    else if (name == "native_link_delete")
    {
        { Id cached(H5Dopen2(file, "source", H5P_DEFAULT), H5Dclose); value(cached, 71); }
        CHECK(H5Ldelete(file, "source", H5P_DEFAULT) >= 0);
        CHECK(H5Ldelete(file, "group", H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "source", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(file, "group", H5P_DEFAULT) == 0);
        { auto recreated = dataset(file, "source", scalar); CHECK(recreated.id >= 0); }
        { Id recreated(H5Gcreate2(file, "group", H5P_DEFAULT, H5P_DEFAULT,
                                  H5P_DEFAULT), H5Gclose); CHECK(recreated.id >= 0); }
    }
    else
    {
        CHECK(H5Lcopy(file, "source", file, "copy", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        { Id copied(H5Dopen2(file, "copy", H5P_DEFAULT), H5Dclose); value(copied, 71); }
        vol.set_passthru(second_filename, "*");
        Id second(H5Fopen(second_filename.c_str(), H5F_ACC_RDWR, connector.fapl), H5Fclose);
        CHECK(H5Lcopy(file, "soft", second, "copied", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lmove(file, "soft2", second, "moved", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(second, "copied", H5P_DEFAULT) > 0);
        CHECK(H5Lexists(second, "moved", H5P_DEFAULT) > 0);
        second.reset();
        std::remove(second_filename.c_str());
    }
}
void run(const std::string& name)
{
    if (name == "wildcard")
    {
        const char subject[] = {'a', '\0', 'x', '\0'};
        CHECK(!LowFive::MetadataVOL::match("a?x", subject));
        CHECK(!LowFive::MetadataVOL::match("?", ""));
        CHECK(LowFive::MetadataVOL::match("a*?", "ab"));
        return;
    }
    if (name == "dataset-metadata-get-failure")
    {
        for (int failure = 1; failure < 4; ++failure)
        {
            int call = 0;
            std::vector<hid_t> created;
            auto result = LowFive::query_dataset_metadata(
                [&](H5VL_dataset_get_args_t& args) -> herr_t
                {
                    if (call++ == failure) return -1;
                    hid_t id = H5I_INVALID_HID;
                    if (args.op_type == H5VL_DATASET_GET_TYPE)
                    {
                        id = H5Tcopy(H5T_NATIVE_INT);
                        args.args.get_type.type_id = id;
                    }
                    else if (args.op_type == H5VL_DATASET_GET_SPACE)
                    {
                        id = H5Screate(H5S_SCALAR);
                        args.args.get_space.space_id = id;
                    }
                    else if (args.op_type == H5VL_DATASET_GET_DCPL)
                    {
                        id = H5Pcreate(H5P_DATASET_CREATE);
                        args.args.get_dcpl.dcpl_id = id;
                    }
                    else
                    {
                        id = H5Pcreate(H5P_DATASET_ACCESS);
                        args.args.get_dapl.dapl_id = id;
                    }
                    CHECK(id >= 0);
                    created.push_back(id);
                    return 0;
                });
            CHECK(!result);
            for (auto id : created) CHECK(H5Iis_valid(id) == 0);
        }
        return;
    }
    if (name == "file_plist_ownership")
    {
        Id fcpl(H5Pcreate(H5P_FILE_CREATE), H5Pclose), fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
        hid_t stored_fcpl, stored_fapl;
        { LowFive::File file("ownership", fcpl.id, fapl.id); stored_fcpl = file.fcpl.id; stored_fapl = file.fapl.id; }
        CHECK(H5Iis_valid(stored_fcpl) == 0); CHECK(H5Iis_valid(stored_fapl) == 0);
        return;
    }
    if (name == "native_datatype_attr" ||
        name == "native_datatype_object_open" ||
        name == "native_datatype_link_create" ||
        name == "native_object_dataset_extent" ||
        name == "native_attr_iteration" || name == "native_attr_open_delete" ||
        name == "native_attr_delete_idx" ||
        name == "native_attr_delete_idx_mixed" ||
        name == "native_link_delete" || name == "native_link_delete_idx" ||
        name == "native_link_delete_idx_uncached" ||
        name == "native_link_copy_move" || name == "native_link_move_cached" ||
        name == "native_link_move_cycle" ||
        name == "native_soft_link_uncached" ||
        name == "native_group_soft_link_uncached" ||
        name == "native_dataset_intermediate_soft_link" ||
        name == "native_object_soft_link_uncached")
    {
        native_callbacks(name);
        return;
    }
    const bool dual = name == "dual_reopen" || name == "extent_failure" ||
                      name == "file-id-child-dual" ||
                      name.find("failure") != std::string::npos;
    const auto mode = name == "file-id-child-file" ? "file" :
                      (dual || name == "native_parent_attr" ? "dual" :
                                                                  "memory");
    test::Connector connector("callbacks_" + name, mode);
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    if (name == "reference-passthru-target" ||
        name == "reference-native-identity" ||
        name == "reference-native-identity-deferred" ||
        name == "reference-after-file-close" ||
        name == "reference-opened-native")
    {
        vol.memory.clear();
        vol.passthru.clear();
        vol.set_memory(connector.filename, "/reference");
        vol.set_passthru(connector.filename, "/native");
    }
    if (name == "file_failure")
    {
        const std::string missing = "/lowfive_nonexistent_parent/callbacks.h5";
        vol.set_passthru(missing, "*"); vol.set_memory(missing, "*");
        CHECK(H5Fcreate(missing.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, connector.fapl) < 0);
        CHECK(vol.files.count(missing) == 0);
        CHECK(H5Fopen(missing.c_str(), H5F_ACC_RDWR, connector.fapl) < 0);
        CHECK(vol.files.count(missing) == 0);
        return;
    }
    auto file = [&]()
    {
        if (name != "reference-opened-native") return connector.file();
        {
            Id native_file(H5Fcreate(connector.filename.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            auto native = dataset(native_file, "native", test::scalar());
            write(native, 107);
        }
        return Id(H5Fopen(connector.filename.c_str(), H5F_ACC_RDWR,
                          connector.fapl), H5Fclose);
    }();
    auto scalar = test::scalar();
    if (name == "lifetime" || name == "multiple_files")
    {
        auto d = dataset(file, "d", scalar); write(d, 17);
        if (name == "multiple_files")
        {
            Id second(H5Fopen(connector.filename.c_str(), H5F_ACC_RDWR, connector.fapl), H5Fclose);
            file.reset();
            Id reopened(H5Dopen2(second, "d", H5P_DEFAULT), H5Dclose); value(reopened, 17);
            second.reset(); value(d, 17);
        }
        else { file.reset(); value(d, 17); write(d, 19); value(d, 19); }
    }
    else if (name == "lifetime_group" || name == "lifetime_attr")
    {
        Id group(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        Id attr(H5Acreate2(group, "a", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int input = 7; CHECK(H5Awrite(attr, H5T_NATIVE_INT, &input) >= 0);
        file.reset();
        if (name == "lifetime_group") { attr.reset(); Id reopened(H5Aopen(group, "a", H5P_DEFAULT), H5Aclose); int output = 0; CHECK(H5Aread(reopened, H5T_NATIVE_INT, &output) >= 0); CHECK(output == input); }
        else { group.reset(); int output = 0; CHECK(H5Aread(attr, H5T_NATIVE_INT, &output) >= 0); CHECK(output == input); }
    }
    else if (name == "file-id-root-group")
    {
        Id root(H5Gopen2(file, "/", H5P_DEFAULT), H5Gclose);
        file.reset();
        Id reopened(H5Iget_file_id(root), H5Fclose);
        CHECK(reopened.id >= 0);
        reopened.reset();
        H5G_info_t info{};
        CHECK(H5Gget_info(root, &info) >= 0);
    }
    else if (name == "file-id-child" || name == "file-id-child-file" ||
             name == "file-id-child-dual")
    {
        auto d = dataset(file, "d", scalar);
        write(d, 29);
        file.reset();
        Id reopened(H5Iget_file_id(d), H5Fclose);
        CHECK(reopened.id >= 0);
        reopened.reset();
        value(d, 29);
    }
    else if (name == "reference-region")
    {
        auto space = test::space(4);
        Id d(H5Dcreate2(file, "d", H5T_NATIVE_INT, space, H5P_DEFAULT,
                        H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        const int values[] = {2, 3, 5, 7};
        CHECK(H5Dwrite(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                       values) >= 0);
        hsize_t start = 1, count = 2;
        CHECK(H5Sselect_hyperslab(space, H5S_SELECT_SET, &start, nullptr,
                                  &count, nullptr) >= 0);
        H5R_ref_t input{};
        CHECK(H5Rcreate_region(file, "d", space, H5P_DEFAULT, &input) >= 0);
        Id attribute(H5Acreate2(file, "region", H5T_STD_REF, scalar,
                                H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(attribute, H5T_STD_REF, &input) >= 0);
        H5R_ref_t output{};
        CHECK(H5Aread(attribute, H5T_STD_REF, &output) >= 0);
        CHECK(H5Rget_type(&output) == H5R_DATASET_REGION2);
        Id selected(H5Ropen_region(&output, H5P_DEFAULT, H5P_DEFAULT),
                    H5Sclose);
        CHECK(selected.id >= 0);
        CHECK(H5Sget_select_npoints(selected) == 2);
        hsize_t bounds[2] = {};
        CHECK(H5Sget_select_bounds(selected, bounds, bounds + 1) >= 0);
        CHECK(bounds[0] == 1 && bounds[1] == 2);
        CHECK(H5Rdestroy(&output) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
    }
    else if (name == "reference-cross-file")
    {
        const std::string second_name = connector.filename + ".target";
        vol.set_memory(second_name, "*");
        Id second(H5Fcreate(second_name.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT,
                            connector.fapl), H5Fclose);
        auto target = dataset(second, "target", scalar);
        write(target, 43);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(second, "target", H5P_DEFAULT, &input) >= 0);
        Id attribute(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(attribute, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        target.reset();
        second.reset();
        H5R_ref_t output{};
        CHECK(H5Aread(attribute, H5T_STD_REF, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 43);
        CHECK(H5Rdestroy(&output) >= 0);
    }
    else if (name == "reference-path-reuse")
    {
        auto original = dataset(file, "original", scalar);
        write(original, 11);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(file, "original", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        original.reset();
        CHECK(H5Lmove(file, "original", file, "renamed", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        auto replacement = dataset(file, "original", scalar);
        write(replacement, 22);

        H5R_ref_t output{};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 11);
        value(replacement, 22);
        CHECK(H5Rdestroy(&output) >= 0);
    }
    else if (name == "reference-deleted-path-reuse")
    {
        auto original = dataset(file, "original", scalar);
        H5R_ref_t input {};
        CHECK(H5Rcreate_object(file, "original", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        original.reset();
        CHECK(H5Ldelete(file, "original", H5P_DEFAULT) >= 0);
        auto replacement = dataset(file, "original", scalar);
        write(replacement, 22);

        H5R_ref_t output {};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) < 0);
        value(replacement, 22);
    }
    else if (name == "reference-passthru-target")
    {
        auto native = dataset(file, "native", scalar);
        write(native, 59);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(file, "native", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        native.reset();
        H5R_ref_t output{};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 59);
        write(opened, 61);
        value(opened, 61);
        Id persisted(H5Dopen2(file, "native", H5P_DEFAULT), H5Dclose);
        value(persisted, 61);
        persisted.reset();
        opened.reset();
        CHECK(H5Rdestroy(&output) >= 0);
        file.reset();

        H5R_ref_t after_file_close{};
        CHECK(H5Aread(holder, H5T_STD_REF, &after_file_close) >= 0);
        Id reopened(H5Ropen_object(&after_file_close, H5P_DEFAULT,
                                   H5P_DEFAULT), H5Oclose);
        CHECK(reopened.id >= 0);
        value(reopened, 61);
        write(reopened, 67);
        value(reopened, 67);
        CHECK(H5Rdestroy(&after_file_close) >= 0);
    }
    else if (name == "reference-native-identity" ||
             name == "reference-native-identity-deferred")
    {
        const bool deferred = name == "reference-native-identity-deferred";
        auto native = dataset(file, "native", scalar);
        write(native, 71);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(file, "native", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);

        H5R_ref_t output{};
        if (!deferred)
        {
            CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
            CHECK(H5Requal(&input, &output) > 0);
            CHECK(H5Rdestroy(&output) >= 0);
        }
        CHECK(H5Lmove(file, "native", file, "renamed", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        native.reset();
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        CHECK(H5Requal(&input, &output) > 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 71);
        CHECK(H5Rdestroy(&output) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
    }
    else if (name == "reference-after-file-close")
    {
        auto native = dataset(file, "native", scalar);
        write(native, 73);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        file.reset();

        H5R_ref_t input{};
        CHECK(H5Rcreate_object(native, ".", H5P_DEFAULT, &input) >= 0);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        native.reset();

        H5R_ref_t output{};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 73);
        CHECK(H5Rdestroy(&output) >= 0);
    }
    else if (name == "reference-opened-native")
    {
        Id native(H5Dopen2(file, "native", H5P_DEFAULT), H5Dclose);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(file, "native", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        native.reset();

        H5R_ref_t output{};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        value(opened, 107);
        CHECK(H5Rdestroy(&output) >= 0);
    }
    else if (name == "reference-registered-uncached")
    {
        const std::string target_name = connector.filename + ".target";
        vol.set_passthru(target_name, "*");
        {
            Id native_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            auto native = dataset(native_file, "native", scalar);
            write(native, 113);
            Id registered(H5Fopen(target_name.c_str(), H5F_ACC_RDWR,
                                  connector.fapl), H5Fclose);

            H5R_ref_t input{};
            CHECK(H5Rcreate_object(native_file, "native", H5P_DEFAULT,
                                   &input) >= 0);
            Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
            CHECK(H5Rdestroy(&input) >= 0);

            H5R_ref_t output{};
            CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
            Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                      H5Oclose);
            value(opened, 113);
            CHECK(H5Rdestroy(&output) >= 0);
        }
        CHECK(std::remove(target_name.c_str()) == 0);
    }
    else if (name == "reference-registered-root")
    {
        const std::string target_name = connector.filename + ".root";
        vol.set_passthru(target_name, "*");
        {
            Id native_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            Id registered(H5Fopen(target_name.c_str(), H5F_ACC_RDWR,
                                  connector.fapl), H5Fclose);

            H5R_ref_t input{};
            CHECK(H5Rcreate_object(native_file, "/", H5P_DEFAULT,
                                   &input) >= 0);
            Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
            CHECK(H5Rdestroy(&input) >= 0);

            H5R_ref_t output{};
            CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
            Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                      H5Oclose);
            CHECK(opened.id >= 0);
            CHECK(H5Iget_type(opened) == H5I_GROUP);
            CHECK(H5Rdestroy(&output) >= 0);
        }
        CHECK(std::remove(target_name.c_str()) == 0);
    }
    else if (name == "reference-registered-uncached-objects" ||
             name == "reference-registered-cached-objects")
    {
        const std::string target_name = connector.filename + ".objects";
        vol.set_passthru(target_name, "*");
        {
            Id native_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            Id group(H5Gcreate2(native_file, "native-group", H5P_DEFAULT,
                                H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            Id datatype(H5Tcopy(H5T_NATIVE_INT), H5Tclose);
            CHECK(H5Tcommit2(native_file, "native-type", datatype,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) >= 0);
            Id registered(H5Fopen(target_name.c_str(), H5F_ACC_RDWR,
                                  connector.fapl), H5Fclose);
            if (name == "reference-registered-cached-objects")
            {
                Id cached_group(H5Gopen2(registered, "native-group",
                                         H5P_DEFAULT), H5Gclose);
                Id cached_type(H5Oopen(registered, "native-type",
                                       H5P_DEFAULT), H5Oclose);
            }

            std::array<H5R_ref_t, 2> input {};
            CHECK(H5Rcreate_object(native_file, "native-group", H5P_DEFAULT,
                                   &input[0]) >= 0);
            CHECK(H5Rcreate_object(native_file, "native-type", H5P_DEFAULT,
                                   &input[1]) >= 0);
            auto pair = test::space(2);
            Id holder(H5Acreate2(file, "references", H5T_STD_REF, pair,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, input.data()) >= 0);
            for (auto& reference : input) CHECK(H5Rdestroy(&reference) >= 0);
            if (name == "reference-registered-cached-objects")
                vol.passthru.clear();

            std::array<H5R_ref_t, 2> output {};
            CHECK(H5Aread(holder, H5T_STD_REF, output.data()) >= 0);
            Id opened_group(H5Ropen_object(&output[0], H5P_DEFAULT,
                                           H5P_DEFAULT), H5Oclose);
            Id gcpl(H5Gget_create_plist(opened_group), H5Pclose);
            CHECK(gcpl.id >= 0);
            Id opened_type(H5Ropen_object(&output[1], H5P_DEFAULT,
                                          H5P_DEFAULT), H5Oclose);
            CHECK(H5Tget_class(opened_type) == H5T_INTEGER);
            for (auto& reference : output) CHECK(H5Rdestroy(&reference) >= 0);
        }
        CHECK(std::remove(target_name.c_str()) == 0);
    }
    else if (name == "reference-registered-intermediate-group")
    {
        const std::string target_name = connector.filename + ".intermediate";
        vol.set_passthru(target_name, "*");
        {
            Id native_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            Id group(H5Gcreate2(native_file, "group", H5P_DEFAULT,
                                H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            auto native = dataset(group, "child", scalar);
            Id registered(H5Fopen(target_name.c_str(), H5F_ACC_RDWR,
                                  connector.fapl), H5Fclose);

            std::array<H5R_ref_t, 2> input {};
            CHECK(H5Rcreate_object(native_file, "group/child", H5P_DEFAULT,
                                   &input[0]) >= 0);
            CHECK(H5Rcreate_object(native_file, "group", H5P_DEFAULT,
                                   &input[1]) >= 0);
            auto pair = test::space(2);
            Id holder(H5Acreate2(file, "references", H5T_STD_REF, pair,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, input.data()) >= 0);
            for (auto& reference : input) CHECK(H5Rdestroy(&reference) >= 0);
            native.reset();
            group.reset();
            registered.reset();
            native_file.reset();
            vol.passthru.clear();

            std::array<H5R_ref_t, 2> output {};
            CHECK(H5Aread(holder, H5T_STD_REF, output.data()) >= 0);
            Id opened(H5Ropen_object(&output[1], H5P_DEFAULT, H5P_DEFAULT),
                      H5Oclose);
            Id gcpl(H5Gget_create_plist(opened), H5Pclose);
            CHECK(gcpl.id >= 0);
            for (auto& reference : output) CHECK(H5Rdestroy(&reference) >= 0);
        }
        CHECK(std::remove(target_name.c_str()) == 0);
    }
    else if (name == "reference-live-file-collection" ||
             name == "reference-retained-collection")
    {
        auto target = dataset(file, "target", scalar);
        write(target, 131);
        H5R_ref_t input{};
        CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &input) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
        CHECK(H5Rdestroy(&input) >= 0);
        target.reset();
        holder.reset();

        if (name == "reference-live-file-collection")
        {
            const std::string other_name = connector.filename + ".other";
            vol.set_memory(other_name, "*");
            {
                Id other(H5Fcreate(other_name.c_str(), H5F_ACC_TRUNC,
                                   H5P_DEFAULT, connector.fapl), H5Fclose);
            }
            CHECK(vol.files.count(connector.filename) == 1);
            Id reopened(H5Dopen2(file, "target", H5P_DEFAULT), H5Dclose);
            value(reopened, 131);
            CHECK(std::remove(other_name.c_str()) != 0);
        }
        else
        {
            file.reset();
            CHECK(vol.files.count(connector.filename) == 0);
        }
    }
    else if (name == "reference-native-attribute")
    {
        const std::string target_name = connector.filename + ".attribute";
        vol.set_passthru(target_name, "*");
        {
            Id native_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            auto native = dataset(native_file, "native", scalar);
            int expected = 137;
            Id label(H5Acreate2(native, "label", H5T_NATIVE_INT, scalar,
                                H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(label, H5T_NATIVE_INT, &expected) >= 0);
            Id registered(H5Fopen(target_name.c_str(), H5F_ACC_RDWR,
                                  connector.fapl), H5Fclose);

            H5R_ref_t input{};
            CHECK(H5Rcreate_attr(native_file, "native", "label", H5P_DEFAULT,
                                 &input) >= 0);
            Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
            CHECK(H5Rdestroy(&input) >= 0);
            label.reset();
            native.reset();
            registered.reset();
            native_file.reset();

            H5R_ref_t output{};
            CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
            Id opened(H5Ropen_attr(&output, H5P_DEFAULT, H5P_DEFAULT),
                      H5Aclose);
            int actual = -1;
            CHECK(H5Aread(opened, H5T_NATIVE_INT, &actual) >= 0);
            CHECK(actual == expected);
            CHECK(H5Rdestroy(&output) >= 0);
        }
        CHECK(std::remove(target_name.c_str()) == 0);
    }
    else if (name == "reference-external-native")
    {
        const std::string external_name = connector.filename + ".external";
        {
            Id external(H5Fcreate(external_name.c_str(), H5F_ACC_TRUNC,
                                  H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
            auto native = dataset(external, "native", scalar);
            write(native, 109);
            H5R_ref_t input{};
            CHECK(H5Rcreate_object(external, "native", H5P_DEFAULT,
                                   &input) >= 0);
            Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                 H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Awrite(holder, H5T_STD_REF, &input) >= 0);
            CHECK(H5Rdestroy(&input) >= 0);

            H5R_ref_t output{};
            CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
            Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                      H5Oclose);
            value(opened, 109);
            CHECK(H5Rdestroy(&output) >= 0);
        }
        CHECK(std::remove(external_name.c_str()) == 0);
    }
    else if (name == "dataset-reference-overwrite")
    {
        Id references(H5Dcreate2(file, "references", H5T_STD_REF, scalar,
                                  H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                      H5Dclose);
        auto obsolete = dataset(file, "obsolete", scalar);
        H5R_ref_t first{};
        CHECK(H5Rcreate_object(file, "obsolete", H5P_DEFAULT, &first) >= 0);
        CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                       H5P_DEFAULT, &first) >= 0);
        CHECK(H5Rdestroy(&first) >= 0);
        obsolete.reset();
        CHECK(H5Ldelete(file, "obsolete", H5P_DEFAULT) >= 0);

        auto current = dataset(file, "current", scalar);
        write(current, 79);
        H5R_ref_t second{};
        CHECK(H5Rcreate_object(file, "current", H5P_DEFAULT, &second) >= 0);
        CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                       H5P_DEFAULT, &second) >= 0);
        CHECK(H5Rdestroy(&second) >= 0);

        H5R_ref_t output{};
        CHECK(H5Dread(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, &output) >= 0);
        Id opened(H5Ropen_object(&output, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        CHECK(opened.id >= 0);
        value(opened, 79);
        CHECK(H5Rdestroy(&output) >= 0);
    }
    else if (name == "reference-zero-copy-alias")
    {
        vol.set_zerocopy(connector.filename, "/references");
        auto target = dataset(file, "target", scalar);
        write(target, 83);
        Id references(H5Dcreate2(file, "references", H5T_STD_REF, scalar,
                                  H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                      H5Dclose);
        H5R_ref_t reference{};
        CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
        CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                       H5P_DEFAULT, &reference) >= 0);
        CHECK(H5Dread(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, &reference) >= 0);
        CHECK(H5Rget_type(&reference) == H5R_OBJECT2);
        Id opened(H5Ropen_object(&reference, H5P_DEFAULT, H5P_DEFAULT),
                  H5Oclose);
        value(opened, 83);
        CHECK(H5Rdestroy(&reference) >= 0);
    }
    else if (name == "vlen-zero-copy-alias")
    {
        vol.set_zerocopy(connector.filename, "/values");
        Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
        Id values(H5Dcreate2(file, "values", type, scalar, H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        int elements[] = {89, 97};
        hvl_t value {2, elements};
        CHECK(H5Dwrite(values, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                       &value) >= 0);
        CHECK(H5Dread(values, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                      &value) >= 0);
        CHECK(value.len == 2 && value.p == elements);
        auto* output = static_cast<int*>(value.p);
        CHECK(output[0] == 89 && output[1] == 97);
    }
    else if (name == "reference-attribute")
    {
        auto d = dataset(file, "d", scalar);
        Id label(H5Acreate2(d, "label", H5T_NATIVE_INT, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int input = 47;
        CHECK(H5Awrite(label, H5T_NATIVE_INT, &input) >= 0);
        H5R_ref_t reference{};
        CHECK(H5Rcreate_attr(file, "d", "label", H5P_DEFAULT,
                             &reference) >= 0);
        Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(holder, H5T_STD_REF, &reference) >= 0);
        H5R_ref_t output{};
        CHECK(H5Aread(holder, H5T_STD_REF, &output) >= 0);
        CHECK(H5Rget_type(&output) == H5R_ATTR);
        std::array<char, 16> name{};
        CHECK(H5Rget_attr_name(&output, name.data(), name.size()) == 5);
        CHECK(std::string(name.data()) == "label");
        Id opened(H5Ropen_attr(&output, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int actual = 0;
        CHECK(H5Aread(opened, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == input);
        CHECK(H5Rdestroy(&output) >= 0);
        CHECK(H5Rdestroy(&reference) >= 0);
    }
    else if (name == "datatype_conversion")
    {
        auto d = dataset(file, "d", scalar);
        float input = 12.75f; CHECK(H5Dwrite(d, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &input) >= 0);
        double output = -1; CHECK(H5Dread(d, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &output) >= 0); CHECK(output == 12.0);
    }
    else if (name == "dual_reopen")
    {
        { auto d = dataset(file, "d", scalar); write(d, 1); }
        { Id d(H5Dopen2(file, "d", H5P_DEFAULT), H5Dclose); write(d, 23); }
        CHECK(H5Fflush(file, H5F_SCOPE_GLOBAL) >= 0);
        file.reset();
        Id native(H5Fopen(connector.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
        Id d(H5Dopen2(native, "d", H5P_DEFAULT), H5Dclose); value(d, 23);
    }
    else if (name == "link_name")
    {
        auto d = dataset(file, "xyz", scalar);
        std::array<char, 5> b{{'L','x','x','x','R'}};
        CHECK(H5Lget_name_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0, b.data()+1, 3, H5P_DEFAULT) == 3);
        CHECK(b[0] == 'L' && b[1] == 'x' && b[2] == 'y' && b[3] == '\0' && b[4] == 'R');
        b.fill('Q'); CHECK(H5Lget_name_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0, b.data()+1, 1, H5P_DEFAULT) == 3);
        CHECK(b[0] == 'Q' && b[1] == '\0' && b[2] == 'Q');
        CHECK(H5Lget_name_by_idx(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0, nullptr, 0, H5P_DEFAULT) == 3);
    }
    else if (name == "group_failure" || name == "dataset_failure" || name == "attr_failure")
    {
        if (name == "group_failure")
        {
            Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            CHECK(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0);
            CHECK(H5Gopen2(file, "missing", H5P_DEFAULT) < 0);
        }
        else if (name == "dataset_failure")
        {
            auto d = dataset(file, "d", scalar);
            CHECK(H5Dcreate2(file, "d", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0);
            CHECK(H5Dopen2(file, "missing", H5P_DEFAULT) < 0);
        }
        else
        {
            Id a(H5Acreate2(file, "a", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
            CHECK(H5Acreate2(file, "a", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT) < 0);
            CHECK(H5Aopen(file, "missing", H5P_DEFAULT) < 0);
        }
        CHECK(vol.files.at(connector.filename)->children.size() == 1);
    }
    else if (name == "missing_dataset") { CHECK(H5Dopen2(file, "absent", H5P_DEFAULT) < 0); CHECK(vol.files.at(connector.filename)->children.empty()); }
    else if (name == "tokens")
    {
        auto d = dataset(file, "d", scalar); write(d, 31);
        H5O_info2_t direct{}, named{};
        CHECK(H5Oget_info3(d, &direct, H5O_INFO_BASIC) >= 0);
        CHECK(H5Oget_info_by_name3(file, "d", &named, H5O_INFO_BASIC, H5P_DEFAULT) >= 0);
        int cmp = 1; CHECK(H5Otoken_cmp(file, &direct.token, &named.token, &cmp) >= 0); CHECK(cmp == 0);
        H5R_ref_t ref{}; CHECK(H5Rcreate_object(file, "d", H5P_DEFAULT, &ref) >= 0);
        Id opened(H5Ropen_object(&ref, H5P_DEFAULT, H5P_DEFAULT), H5Oclose);
        CHECK(H5Iget_type(opened) == H5I_DATASET); value(opened, 31); CHECK(H5Rdestroy(&ref) >= 0);
    }
    else if (name == "attr_rename")
    {
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        Id a(H5Acreate2(g, "old", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose); a.reset();
        CHECK(H5Arename(g, "old", "new") >= 0); CHECK(H5Aexists(g, "old") == 0); CHECK(H5Aexists(g, "new") > 0);
        Id same(H5Gopen2(file, "g", H5P_DEFAULT), H5Gclose);
        CHECK(H5Arename(g, "absent", "other") < 0);
    }
    else if (name == "hard_link_unlink")
    {
        auto d = dataset(file, "d", scalar);
        write(d, 41);
        CHECK(H5Lcreate_hard(file, "d", file, "alias", H5P_DEFAULT,
                             H5P_DEFAULT) >= 0);
        d.reset();
        CHECK(H5Ldelete(file, "d", H5P_DEFAULT) >= 0);
        Id alias(H5Dopen2(file, "alias", H5P_DEFAULT), H5Dclose);
        value(alias, 41);
    }
    else if (name == "open_unlink_read")
    {
        auto d = dataset(file, "d", scalar);
        write(d, 51);
        CHECK(H5Ldelete(file, "d", H5P_DEFAULT) >= 0);
        value(d, 51);
        CHECK(H5Lexists(file, "d", H5P_DEFAULT) == 0);
    }
    else if (name == "anonymous_dataset_link")
    {
        Id anonymous(H5Dcreate_anon(file, H5T_NATIVE_INT, scalar,
                                    H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        write(anonymous, 67);
        CHECK(H5Olink(anonymous, file, "named", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        H5G_info_t info{};
        CHECK(H5Gget_info(file, &info) >= 0);
        CHECK(info.nlinks == 1);
        Id named(H5Dopen2(file, "named", H5P_DEFAULT), H5Dclose);
        value(named, 67);
        CHECK(H5Ldelete(file, "named", H5P_DEFAULT) >= 0);
        CHECK(H5Gget_info(file, &info) >= 0);
        CHECK(info.nlinks == 0);
        value(anonymous, 67);
    }
    else if (name == "detached-self-link")
    {
        Id group(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT), H5Gclose);
        CHECK(H5Ldelete(file, "g", H5P_DEFAULT) >= 0);
        CHECK(H5Olink(group, group, "self", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        Id self(H5Gopen2(group, "self", H5P_DEFAULT), H5Gclose);
        CHECK(self.id >= 0);
    }
    else if (name == "hard_links" || name == "soft_links" || name == "absolute_paths")
    {
        auto d = dataset(file, "d", scalar); write(d, 41);
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        if (name == "hard_links") { CHECK(H5Olink(d, g, "alias", H5P_DEFAULT, H5P_DEFAULT) >= 0); Id original(H5Dopen2(file, "d", H5P_DEFAULT), H5Dclose); Id alias(H5Dopen2(g, "alias", H5P_DEFAULT), H5Dclose); value(original, 41); value(alias, 41); CHECK(H5Lcreate_hard(file, "g", file, "group_alias", H5P_DEFAULT, H5P_DEFAULT) >= 0); Id nested(H5Dopen2(file, "group_alias/alias", H5P_DEFAULT), H5Dclose); value(nested, 41); }
        else if (name == "soft_links") { CHECK(H5Lcreate_soft("/d", g, "absolute", H5P_DEFAULT, H5P_DEFAULT) >= 0); CHECK(H5Lcreate_soft("absolute", g, "relative", H5P_DEFAULT, H5P_DEFAULT) >= 0); Id alias(H5Dopen2(g, "relative", H5P_DEFAULT), H5Dclose); value(alias, 41); CHECK(H5Lcreate_soft("cycle", g, "cycle", H5P_DEFAULT, H5P_DEFAULT) >= 0); CHECK(H5Dopen2(g, "cycle", H5P_DEFAULT) < 0); }
        else { auto local = dataset(g, "d", scalar); write(local, 99); Id absolute(H5Dopen2(g, "/d", H5P_DEFAULT), H5Dclose); value(absolute, 41); }
    }
    else if (name == "link_copy_move")
    {
        auto d = dataset(file, "d", scalar); write(d, 41);
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        CHECK(H5Lcreate_soft("/d", g, "source", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lcopy(g, "source", g, "copy", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(g, "source", H5P_DEFAULT) > 0);
        CHECK(H5Lexists(g, "copy", H5P_DEFAULT) > 0);
        { Id alias(H5Dopen2(g, "copy", H5P_DEFAULT), H5Dclose); value(alias, 41); }
        CHECK(H5Lmove(g, "source", g, "moved", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(g, "source", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(g, "moved", H5P_DEFAULT) > 0);
        { Id alias(H5Dopen2(g, "moved", H5P_DEFAULT), H5Dclose); value(alias, 41); }
        CHECK(H5Ldelete(g, "copy", H5P_DEFAULT) >= 0);
        CHECK(H5Ldelete(g, "moved", H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(g, "copy", H5P_DEFAULT) == 0);
        CHECK(H5Lexists(g, "moved", H5P_DEFAULT) == 0);
    }
    else if (name == "soft-link-cross-file")
    {
        const std::string second_name = connector.filename + ".second";
        vol.set_memory(second_name, "*");
        Id second(H5Fcreate(second_name.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT,
                            connector.fapl), H5Fclose);
        auto copied_target = dataset(second, "copy-target", scalar);
        auto moved_target = dataset(second, "move-target", scalar);
        write(copied_target, 101);
        write(moved_target, 103);
        CHECK(H5Lcreate_soft("/copy-target", file, "copy-source",
                             H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lcreate_soft("/move-target", file, "move-source",
                             H5P_DEFAULT, H5P_DEFAULT) >= 0);
        CHECK(H5Lcopy(file, "copy-source", second, "copied", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        CHECK(H5Lmove(file, "move-source", second, "moved", H5P_DEFAULT,
                      H5P_DEFAULT) >= 0);
        CHECK(H5Lexists(file, "copy-source", H5P_DEFAULT) > 0);
        CHECK(H5Lexists(file, "move-source", H5P_DEFAULT) == 0);
        Id copied(H5Dopen2(second, "copied", H5P_DEFAULT), H5Dclose);
        Id moved(H5Dopen2(second, "moved", H5P_DEFAULT), H5Dclose);
        value(copied, 101);
        value(moved, 103);
    }
    else if (name == "attr_by_name" || name == "native_parent_attr")
    {
        if (name == "native_parent_attr") { vol.passthru.clear(); vol.set_passthru(connector.filename, "/g"); }
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        Id a(H5Acreate_by_name(file, "/g", "a", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int input = 53; CHECK(H5Awrite(a, H5T_NATIVE_INT, &input) >= 0); a.reset();
        Id reopened(H5Aopen_by_name(file, "/g", "a", H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int output = 0; CHECK(H5Aread(reopened, H5T_NATIVE_INT, &output) >= 0); CHECK(output == input);
        CHECK(H5Aexists(file, "a") == 0);
    }
    else if (name == "file_plists")
    {
        for (int i = 0; i < 4; ++i)
        {
            Id fcpl(H5Fget_create_plist(file), H5Pclose), fapl(H5Fget_access_plist(file), H5Pclose);
            hsize_t size = 1; CHECK(H5Pget_userblock(fcpl, &size) >= 0); CHECK(size == 0);
            CHECK(H5Pset_userblock(fcpl, 512) >= 0); CHECK(H5Pset_alignment(fapl, 1, 4096) >= 0);
        }
    }
    else if (name == "enumeration" || name == "indexed_lookup")
    {
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        Id a(H5Acreate2(g, "attr", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        auto z = dataset(g, "z", scalar); write(z, 1); auto b = dataset(g, "b", scalar); write(b, 2);
        if (name == "enumeration") { H5G_info_t info{}; CHECK(H5Gget_info(g, &info) >= 0); CHECK(info.nlinks == 2); std::vector<std::string> names; hsize_t idx = 0; CHECK(H5Literate2(g, H5_INDEX_NAME, H5_ITER_INC, &idx, [](hid_t, const char* n, const H5L_info2_t*, void* p) -> herr_t { static_cast<std::vector<std::string>*>(p)->emplace_back(n); return 0; }, &names) >= 0); CHECK(names.size() == 2); CHECK(names[0] == "b" && names[1] == "z"); }
        else { Id first(H5Oopen_by_idx(file, "g", H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT), H5Oclose); value(first, 2); Id last(H5Oopen_by_idx(file, "g", H5_INDEX_NAME, H5_ITER_DEC, 0, H5P_DEFAULT), H5Oclose); value(last, 1); CHECK(H5Oopen_by_idx(file, "g", H5_INDEX_NAME, H5_ITER_INC, 2, H5P_DEFAULT) < 0); }
    }
    else if (name == "object_names")
    {
        Id g(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose); auto d = dataset(g, "d", scalar);
        std::array<char, 16> b; b.fill('Q'); CHECK(H5Iget_name(d, b.data(), b.size()) == 4); CHECK(std::string(b.data()) == "/g/d");
        b.fill('Q'); CHECK(H5Iget_name(d, b.data(), 3) == 4); CHECK(b[0] == '/' && b[1] == 'g' && b[2] == '\0' && b[3] == 'Q');
    }
    else if (name == "zero_file_name") { char guard[3] = {'L','X','R'}; CHECK(H5Fget_name(file, guard+1, 0) == static_cast<ssize_t>(connector.filename.size())); CHECK(guard[0] == 'L' && guard[1] == 'X' && guard[2] == 'R'); }
    else if (name == "extent_failure") { auto space = test::space(4); auto d = dataset(file, "d", space); hsize_t smaller = 2; CHECK(H5Dset_extent(d, &smaller) < 0); auto* metadata = dynamic_cast<LowFive::Dataset*>(vol.locate(connector.filename, "/d")); CHECK(metadata); CHECK(metadata->space.size() == 4); }
    else if (name == "reference_ids")
    {
        auto d = dataset(file, "d", scalar); write(d, 9);
        Id a(H5Acreate2(file, "a", H5T_NATIVE_INT, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int attribute_value = 4; CHECK(H5Awrite(a, H5T_NATIVE_INT, &attribute_value) >= 0);
        auto* root = dynamic_cast<LowFive::File*>(vol.files.at(connector.filename));
        auto* object = vol.locate(connector.filename, "/d");
        const auto before = H5Fget_obj_count(file, H5F_OBJ_DATASET);
        H5R_ref_t ref{};
        root->references.emplace(
            &ref, LowFive::File::PendingReference {
                H5R_OBJECT2, root->name, object->token
            });
        CHECK(H5Aread(a, H5T_NATIVE_INT, &attribute_value) >= 0);
        CHECK(H5Fget_obj_count(file, H5F_OBJ_DATASET) == before);
        CHECK(H5Rdestroy(&ref) >= 0);
    }
    else CHECK(false);
}
}
int main(int argc, char** argv) { return test::run(argc, argv, run); }
