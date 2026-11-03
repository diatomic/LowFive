#include "test.hpp"
#include "metadata.hpp"
#include "metadata/dummy.hpp"
#include "vol-metadata-private.hpp"
#include <lowfive/vol-dist-metadata.hpp>
#include <array>
#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>
#include <limits>


struct VLenRecord
{
    int tag;
    hvl_t values;
};

test::Id vlen_record_type()
{
    test::Id type(H5Tcreate(H5T_COMPOUND, sizeof(VLenRecord)), H5Tclose);
    test::Id values(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    CHECK(H5Tinsert(type, "tag", offsetof(VLenRecord, tag),
                    H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(type, "values", offsetof(VLenRecord, values),
                    values) >= 0);
    return type;
}

struct WideVLenRecord
{
    int tag;
    hvl_t values;
    int sentinel;
    hvl_t preserved;
};

test::Id wide_vlen_record_type()
{
    test::Id type(
        H5Tcreate(H5T_COMPOUND, sizeof(WideVLenRecord)), H5Tclose);
    test::Id values(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    CHECK(H5Tinsert(type, "tag", offsetof(WideVLenRecord, tag),
                    H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(type, "values", offsetof(WideVLenRecord, values),
                    values) >= 0);
    CHECK(H5Tinsert(type, "sentinel", offsetof(WideVLenRecord, sentinel),
                    H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(type, "preserved", offsetof(WideVLenRecord, preserved),
                    values) >= 0);
    return type;
}
namespace
{
void exercise_intercomm_indices()
{
    auto& vol = LowFive::DistMetadataVOL::create_DistMetadataVOL(
        MPI_COMM_SELF, LowFive::DistMetadataVOL::communicators { MPI_COMM_SELF });
    vol.serve_on_close = false;
    CHECK(vol.remote_size(0) == 1);
    const auto rejects = [](const auto& operation)
    {
        bool rejected = false;
        try { operation(); }
        catch (const std::out_of_range&) { rejected = true; }
        CHECK(rejected);
    };
    for (int index : { -1, 1, std::numeric_limits<int>::max() })
    {
        rejects([&] { vol.set_intercomm("indices.h5", "*", index); });
        CHECK(vol.intercomm_locations.empty());
        CHECK(vol.intercomm_indices.empty());
        rejects([&] { vol.remote_size(index); });
        rejects([&] { vol.get_filenames(index); });
        rejects([&] { vol.send_done(index); });
        vol.set_serve_indices([index] { return LowFive::DistMetadataVOL::DataIntercomms { 0, index }; });
        rejects([&] { vol.serve_all(false); });
        rejects([&] { vol.producer_signal_done(); });
    }
    vol.unset_dist_callbacks();
    vol.set_intercomm("indices.h5", "*", 0);
    vol.set_memory("indices.h5", "*");
    const hid_t connector = vol.register_plugin();
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    vol.intercomms.clear();
    CHECK(H5Fopen("indices.h5", H5F_ACC_RDONLY, fapl) < 0);
    fapl.reset();
    CHECK(H5VLunregister_connector(connector) >= 0);
}

void exercise_serving_cleanup(int rank, MPI_Comm local, MPI_Comm intercomm)
{
    auto& vol = LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomm);
    vol.keep = rank == 0;
    vol.serve_on_close = false;
    const std::string filename = "distributed-serving-cleanup.h5";
    vol.set_memory(filename, "*");
    vol.set_intercomm(filename, "*", 0);
    const hid_t connector = vol.register_plugin();
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    for (int step = 0; step < 3; ++step)
    {
        const int expected = step == 2 ? 73 : 42;
        if (rank == 0)
        {
            test::Id file(
                step == 1 ? H5Fopen(filename.c_str(), H5F_ACC_RDWR, fapl) :
                            H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl),
                H5Fclose);
            auto space = test::space(1);
            test::Id dataset(
                step == 1 ? H5Dopen2(file, "values", H5P_DEFAULT) :
                            H5Dcreate2(file, "values", H5T_NATIVE_INT, space,
                                       H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                H5Dclose);
            if (step != 1)
                CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                               H5P_DEFAULT, &expected) >= 0);
            auto* metadata = dynamic_cast<LowFive::Dataset*>(vol.locate(filename, "/values"));
            CHECK(metadata);
            const hid_t stored_dcpl = metadata->dcpl.id;
            if (step == 0)
            {
                dataset.reset();
                file.reset();
                vol.serve_all(false);
                CHECK(vol.files.count(filename) == 1);
                CHECK(metadata->extra == nullptr);
                CHECK(H5Iis_valid(stored_dcpl) > 0);
            }
            else if (step == 1)
            {
                vol.serve_all();
                CHECK(vol.files.empty());
                CHECK(metadata->extra == nullptr);
                int actual = 0;
                CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                              H5P_DEFAULT, &actual) >= 0);
                CHECK(actual == expected);
                file.reset();
                CHECK(H5Iis_valid(stored_dcpl) > 0);
                dataset.reset();
                CHECK(H5Iis_valid(stored_dcpl) == 0);
            }
            else
            {
                dataset.reset();
                vol.serve_on_close = true;
                file.reset();
                CHECK(vol.files.empty());
                CHECK(H5Iis_valid(stored_dcpl) == 0);
            }
        }
        else
        {
            test::Id file(H5Fopen(filename.c_str(), H5F_ACC_RDONLY, fapl), H5Fclose);
            test::Id dataset(H5Dopen2(file, "values", H5P_DEFAULT), H5Dclose);
            int actual = 0;
            CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                          H5P_DEFAULT, &actual) >= 0);
            CHECK(actual == expected);
        }
        CHECK(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
    }
    fapl.reset();
    CHECK(H5VLunregister_connector(connector) >= 0);
}

int copies = 0;
int copy_comm(MPI_Comm, int, void*, void* attribute, void* output, int* flag)
{
    *static_cast<void**>(output) = attribute;
    *flag = 1;
    ++copies;
    return MPI_SUCCESS;
}
int delete_comm(MPI_Comm, int, void*, void*) { --copies; return MPI_SUCCESS; }

void select(hid_t space, hsize_t start, hsize_t count, hsize_t stride = 1)
{
    CHECK(H5Sselect_hyperslab(space, H5S_SELECT_SET, &start, &stride, &count, nullptr) >= 0);
}

void exercise_broadcast(int rank)
{
    auto& vol = LowFive::DistMetadataVOL::create_DistMetadataVOL(
        MPI_COMM_WORLD, LowFive::DistMetadataVOL::communicators {});
    vol.serve_on_close = false;
    vol.keep = true;
    const std::string filename = "distributed-broadcast.h5";
    const std::string stale = "distributed-broadcast-stale.h5";
    vol.set_memory(filename, "*");
    vol.set_memory(stale, "*");
    const hid_t connector = vol.register_plugin();
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    auto create = [&](hsize_t size, int fill)
    {
        test::Id file(H5Fcreate(filename.c_str(), H5F_ACC_TRUNC,
                                H5P_DEFAULT, fapl), H5Fclose);
        auto extent = test::space(size);
        test::Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
        CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &fill) >= 0);
        test::Id dataset(H5Dcreate2(file, "values", H5T_NATIVE_INT, extent,
                                    H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
        H5R_ref_t reference {};
        CHECK(H5Rcreate_object(file, "values", H5P_DEFAULT, &reference) >= 0);
        auto scalar = test::scalar();
        test::Id attribute(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                                      H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        CHECK(H5Awrite(attribute, H5T_STD_REF, &reference) >= 0);
        CHECK(H5Rdestroy(&reference) >= 0);
    };
    auto check = [&](hid_t dataset, hsize_t size, int fill)
    {
        test::Id extent(H5Dget_space(dataset), H5Sclose);
        hsize_t actual = 0;
        CHECK(H5Sget_simple_extent_dims(extent, &actual, nullptr) == 1);
        CHECK(actual == size);
        std::vector<int> values(size + 2, -91);
        CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, values.data() + 1) >= 0);
        CHECK(values.front() == -91 && values.back() == -91);
        CHECK(std::all_of(values.begin() + 1, values.end() - 1,
                          [&](int value) { return value == fill; }));
    };
    auto check_reference = [&](hid_t attribute, hsize_t size, int fill)
    {
        H5R_ref_t reference {};
        CHECK(H5Aread(attribute, H5T_STD_REF, &reference) >= 0);
        test::Id target(H5Ropen_object(&reference, H5P_DEFAULT, H5P_DEFAULT),
                         H5Dclose);
        check(target, size, fill);
        CHECK(H5Rdestroy(&reference) >= 0);
    };
    if (rank == 0)
        create(1, 11);
    else
    {
        test::Id old(H5Fcreate(stale.c_str(), H5F_ACC_TRUNC,
                               H5P_DEFAULT, fapl), H5Fclose);
    }
    vol.broadcast_files();
    CHECK(vol.files.size() == 1 && vol.files.count(filename) == 1);
    test::Id old_file(H5Fopen(filename.c_str(), H5F_ACC_RDONLY, fapl),
                      H5Fclose);
    test::Id old_dataset(H5Dopen2(old_file, "values", H5P_DEFAULT), H5Dclose);
    test::Id old_attribute(H5Aopen(old_file, "reference", H5P_DEFAULT),
                            H5Aclose);
    const hid_t old_dcpl =
        dynamic_cast<LowFive::Dataset*>(vol.locate(filename, "/values"))->dcpl.id;
    check(old_dataset, 1, 11);
    check_reference(old_attribute, 1, 11);
    for (hsize_t step = 2; step <= 4; ++step)
    {
        hid_t replaced_dcpl = H5I_INVALID_HID;
        if (rank == 0)
            create(step, int(10 + step));
        else if (step > 2)
            replaced_dcpl = dynamic_cast<LowFive::Dataset*>(
                vol.locate(filename, "/values"))->dcpl.id;
        vol.broadcast_files();
        CHECK(vol.files.size() == 1 && vol.files.count(stale) == 0);
        if (replaced_dcpl >= 0)
            CHECK(H5Iis_valid(replaced_dcpl) == 0);
        test::Id current_file(
            H5Fopen(filename.c_str(), H5F_ACC_RDONLY, fapl), H5Fclose);
        test::Id current_dataset(
            H5Dopen2(current_file, "values", H5P_DEFAULT), H5Dclose);
        test::Id current_attribute(H5Aopen(current_file, "reference", H5P_DEFAULT),
                                    H5Aclose);
        check_reference(current_attribute, step, int(10 + step));
        check(current_dataset, step, int(10 + step));
        check(old_dataset, 1, 11);
        check_reference(old_attribute, 1, 11);
        CHECK(H5Iis_valid(old_dcpl) > 0);
    }
    old_file.reset();
    check(old_dataset, 1, 11);
    check_reference(old_attribute, 1, 11);
    old_attribute.reset();
    old_dataset.reset();
    CHECK(H5Iis_valid(old_dcpl) == 0);
    const hid_t final_dcpl =
        dynamic_cast<LowFive::Dataset*>(vol.locate(filename, "/values"))->dcpl.id;
    if (rank == 0)
        vol.clear_files();
    vol.broadcast_files();
    CHECK(vol.files.empty());
    CHECK(H5Iis_valid(final_dcpl) == 0);
    fapl.reset();
    CHECK(H5VLunregister_connector(connector) >= 0);
}

void exercise(const std::string& name, int rank, MPI_Comm local, MPI_Comm intercomm)
{
    auto& vol = LowFive::DistMetadataVOL::create_DistMetadataVOL(local, intercomm);
    vol.serve_on_close = false;
    vol.keep = rank == 0;
    const std::string first = "distributed-" + name + "-a.h5";
    const std::string second = "distributed-" + name + "-b.h5";
    const std::string absent = "distributed-" + name + "-absent.h5";
    vol.set_memory(first, name == "object-alias-policy" ? "/values" : "*");
    vol.set_memory(second, "*");
    vol.set_intercomm(first, "*", 0);
    if (name == "remote-missing")
    {
        vol.set_memory(absent, "*");
        vol.set_intercomm(absent, "*", 0);
    }
    if (name == "native-missing")
        vol.set_passthru(first, "*");
    vol.set_intercomm(second, "*", 0);
    const hid_t connector = vol.register_plugin();
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    int key = MPI_KEYVAL_INVALID;
    if (name == "mpio-resources")
    {
        CHECK(MPI_Comm_create_keyval(copy_comm, delete_comm, &key, nullptr) == MPI_SUCCESS);
        CHECK(MPI_Comm_set_attr(local, key, &copies) == MPI_SUCCESS);
        copies = 1;
        MPI_Info info;
        CHECK(MPI_Info_create(&info) == MPI_SUCCESS);
        CHECK(MPI_Info_set(info, "test_key", "test_value") == MPI_SUCCESS);
        CHECK(H5Pset_fapl_mpio(fapl, local, info) >= 0);
        CHECK(MPI_Info_free(&info) == MPI_SUCCESS);
    }
    CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    const std::array<double, 4> expected = {{1.25, 2.5, 3.75, 5.0}};
    auto create = [&](const std::string& filename)
    {
        test::Id file(H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose);
        if (name == "scalar")
        {
            test::Id scalar(H5Screate(H5S_SCALAR), H5Sclose);
            test::Id ds(H5Dcreate2(file, "values", H5T_NATIVE_INT, scalar,
                                   H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                        H5Dclose);
            int value = 37;
            CHECK(H5Dwrite(ds, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                           H5P_DEFAULT, &value) >= 0);
            return;
        }
        if (name == "fill-unwritten" || name == "fill-partial" ||
            name == "fill-default")
        {
            auto extent = test::space(4);
            test::Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
            const double fill = -7.5;
            if (name != "fill-default")
                CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_DOUBLE, &fill) >= 0);
            test::Id ds(H5Dcreate2(file, "values", H5T_NATIVE_DOUBLE,
                                   extent, H5P_DEFAULT, dcpl, H5P_DEFAULT),
                        H5Dclose);
            if (name != "fill-unwritten")
            {
                auto packed = test::space(2);
                select(extent, 1, 2, 2);
                const double values[] = {expected[1], expected[3]};
                CHECK(H5Dwrite(ds, H5T_NATIVE_DOUBLE, packed, extent,
                               H5P_DEFAULT, values) >= 0);
            }
            return;
        }
        if (name == "fill-vlen")
        {
            auto extent = test::space(3);
            test::Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
            test::Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
            int fill_values[] = {8, 9};
            hvl_t fill {2, fill_values};
            CHECK(H5Pset_fill_value(dcpl, type, &fill) >= 0);
            test::Id ds(H5Dcreate2(file, "values", type, extent,
                                   H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
            auto one = test::space(1);
            select(extent, 1, 1);
            int written[] = {42};
            hvl_t value {1, written};
            CHECK(H5Dwrite(ds, type, one, extent, H5P_DEFAULT, &value) >= 0);
            return;
        }
        if (name == "empty-string")
        {
            auto extent = test::space(2);
            test::Id type(H5Tcopy(H5T_C_S1), H5Tclose);
            CHECK(H5Tset_size(type, H5T_VARIABLE) >= 0);
            test::Id ds(H5Dcreate2(file, "values", type, extent, H5P_DEFAULT,
                                   H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
            const char* values[] = {"", "written"};
            CHECK(H5Dwrite(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           values) >= 0);
            return;
        }
        if (name == "fill-string")
        {
            auto extent = test::space(3);
            test::Id type(H5Tcopy(H5T_C_S1), H5Tclose);
            CHECK(H5Tset_size(type, H5T_VARIABLE) >= 0);
            test::Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
            const char* fill = "unwritten";
            CHECK(H5Pset_fill_value(dcpl, type, &fill) >= 0);
            test::Id ds(H5Dcreate2(file, "values", type, extent,
                                   H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
            auto one = test::space(1);
            select(extent, 1, 1);
            const char* value = "written";
            CHECK(H5Dwrite(ds, type, one, extent, H5P_DEFAULT, &value) >= 0);
            return;
        }
        if (name == "vlen")
        {
            auto extent = test::space(1);
            test::Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
            test::Id ds(H5Dcreate2(file, "values", type, extent, H5P_DEFAULT,
                                   H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
            int values[] = {4, 5, 6};
            hvl_t value {3, values};
            CHECK(H5Dwrite(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           &value) >= 0);
            return;
        }
        if (name == "vlen-compound")
        {
            auto extent = test::space(1);
            auto type = vlen_record_type();
            test::Id ds(H5Dcreate2(file, "values", type, extent, H5P_DEFAULT,
                                   H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
            int values[] = {7, 8, 9};
            VLenRecord value {42, hvl_t {3, values}};
            CHECK(H5Dwrite(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           &value) >= 0);
            return;
        }
        if (name == "vlen-compound-selection")
        {
            auto extent = test::space(2);
            auto type = vlen_record_type();
            test::Id ds(H5Dcreate2(file, "values", type, extent, H5P_DEFAULT,
                                   H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
            int first_values[] = {1, 2};
            int second_values[] = {3, 4, 5};
            VLenRecord values[] = {
                {10, hvl_t {2, first_values}},
                {20, hvl_t {3, second_values}}
            };
            CHECK(H5Dwrite(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           values) >= 0);

            auto one = test::space(1);
            select(extent, 0, 1);
            int replacement_values[] = {6};
            VLenRecord replacement {11, hvl_t {1, replacement_values}};
            CHECK(H5Dwrite(ds, type, one, extent, H5P_DEFAULT,
                           &replacement) >= 0);
            return;
        }
        auto extent = test::space(4);
        test::Id ds(H5Dcreate2(file, "values", H5T_NATIVE_DOUBLE, extent, H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        if (name == "irregular")
        {
            auto packed = test::space(2);
            select(extent, 0, 2, 2);
            const double even[] = {expected[0], expected[2]};
            CHECK(H5Dwrite(ds, H5T_NATIVE_DOUBLE, packed, extent, H5P_DEFAULT, even) >= 0);
            select(extent, 1, 2, 2);
            const double odd[] = {expected[1], expected[3]};
            CHECK(H5Dwrite(ds, H5T_NATIVE_DOUBLE, packed, extent, H5P_DEFAULT, odd) >= 0);
        }
        else
            CHECK(H5Dwrite(ds, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, expected.data()) >= 0);
        if (name == "hard-link-parent-relative")
        {
            test::Id group(H5Gcreate2(file, "g", H5P_DEFAULT, H5P_DEFAULT,
                                      H5P_DEFAULT), H5Gclose);
            test::Id nested(H5Dcreate2(group, "d", H5T_NATIVE_DOUBLE, extent,
                                       H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                            H5Dclose);
            CHECK(H5Dwrite(nested, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                           H5P_DEFAULT, expected.data()) >= 0);
            CHECK(H5Olink(group, file, "alias", H5P_DEFAULT,
                          H5P_DEFAULT) >= 0);
            CHECK(H5Ldelete(file, "g", H5P_DEFAULT) >= 0);
        }
        if (name == "hard-link-replace" || name == "hard-link-serialized" ||
            name == "object-hard-link-serialized" ||
            name == "object-alias-policy")
            CHECK(H5Olink(ds, file, "alias", H5P_DEFAULT, H5P_DEFAULT) >= 0);
        if (name == "hard-link-serialized" || name == "object-hard-link-serialized")
            CHECK(H5Ldelete(file, "values", H5P_DEFAULT) >= 0);
    };
    const int steps = name == "mpio-resources" ? 8 : 1;
    for (int step = 0; step < steps; ++step)
    {
        if (rank == 0)
        {
            create(first);
            if (name == "native-missing")
                CHECK(std::remove(first.c_str()) == 0);
            if (name == "multifile") create(second);
            if (name == "copy-whole" || name == "dummy-copy-whole")
            {
                auto* source =
                    dynamic_cast<LowFive::File*>(vol.files.at(first));
                CHECK(source);
                source->copy_whole = true;
            }
            vol.serve_all(true, name != "no-index");
            CHECK(vol.files.empty());
        }
        else if (name == "native-missing")
        {
            CHECK(H5Fopen(first.c_str(), H5F_ACC_RDONLY, fapl) < 0);
        }
        else if (name == "remote-missing")
        {
            CHECK(H5Fopen(absent.c_str(), H5F_ACC_RDONLY, fapl) < 0);
        }
        else
        {
            if (name == "copy-whole" || name == "dummy-copy-whole")
            {
                int local_rank = 0;
                CHECK(MPI_Comm_rank(local, &local_rank) == MPI_SUCCESS);
                if (local_rank == 1)
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(100));
            }
            LowFive::ObjectPointers* dummy_wrapper = nullptr;
            hid_t dummy_dcpl = H5I_INVALID_HID;
            if (name == "dummy-real" || name == "dummy-copy-whole")
            {
                auto* dummy = new LowFive::DummyFile(first);
                auto extent = test::space(1);
                auto* child = new LowFive::Dataset(
                    "old", H5T_NATIVE_INT, extent,
                    LowFive::Dataset::Ownership::lowfive,
                    H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT,
                    false, true);
                dummy->add_child(child);
                dummy_dcpl = child->dcpl.id;
                CHECK(vol.files.emplace(first, dummy).second);
                dummy_wrapper = static_cast<LowFive::ObjectPointers*>(
                    vol.MetadataVOL::file_open(
                        first.c_str(), H5F_ACC_RDONLY, fapl,
                        H5P_DEFAULT, nullptr));
                CHECK(dummy_wrapper && dummy_wrapper->mdata_obj == dummy);
            }
            const unsigned open_flags =
                name == "remote-unlinked-alias" ? H5F_ACC_RDWR :
                                                  H5F_ACC_RDONLY;
            const hid_t file_id = H5Fopen(first.c_str(), open_flags, fapl);
            CHECK(file_id >= 0);
            test::Id file(file_id, H5Fclose);
            const bool object_alias = name == "object-hard-link-serialized" ||
                                      name == "object-alias-policy";
            const bool detached_alias = name == "hard-link-serialized" ||
                                        name == "object-hard-link-serialized";
            const char* dataset_name =
                detached_alias || name == "object-alias-policy" ?
                    "alias" : "values";
            const hid_t dataset_id =
                object_alias ? H5Oopen(file, dataset_name, H5P_DEFAULT) :
                               H5Dopen2(file, dataset_name, H5P_DEFAULT);
            CHECK(dataset_id >= 0);
            test::Id ds(dataset_id, object_alias ? H5Oclose : H5Dclose);
            auto read_all = [&](hid_t dataset)
            {
                std::array<double, 6> result;
                result.fill(-91);
                CHECK(H5Dread(dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, result.data() + 1) >= 0);
                CHECK(result.front() == -91 && result.back() == -91);
                CHECK(std::equal(expected.begin(), expected.end(), result.begin() + 1));
            };
            if (name == "no-index")
            {
                test::Id type(H5Dget_type(ds), H5Tclose);
                test::Id space(H5Dget_space(ds), H5Sclose);
                CHECK(H5Tequal(type, H5T_NATIVE_DOUBLE) > 0);
                CHECK(H5Sget_simple_extent_ndims(space) == 1);
            }
            else if (name == "fill-unwritten" || name == "fill-partial" ||
                     name == "fill-default")
            {
                const float fill = name == "fill-default" ? 0 : -7.5f;
                auto expected_value = [&](size_t i)
                {
                    return name != "fill-unwritten" && i % 2 == 1 ?
                               float(expected[i]) : fill;
                };
                std::array<float, 8> result;
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL,
                              H5P_DEFAULT, result.data() + 1) >= 0);
                for (size_t i = 0; i < result.size(); ++i)
                    CHECK(result[i] == (i >= 1 && i <= 4 ?
                                            expected_value(i - 1) : -91));
                auto fs = test::space(4);
                select(fs, 0, 3);
                auto ms = test::space(6);
                select(ms, 1, 3, 2);
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_FLOAT, ms, fs,
                              H5P_DEFAULT, result.data() + 1) >= 0);
                for (size_t i = 0; i < result.size(); ++i)
                    CHECK(result[i] == (i == 2 || i == 4 || i == 6 ?
                                            expected_value((i - 2) / 2) : -91));
            }
            else if (name == "fill-vlen")
            {
                test::Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
                auto fs = test::space(3);
                auto ms = test::space(7);
                select(ms, 1, 3, 2);
                std::array<hvl_t, 7> result {};
                CHECK(H5Dread(ds, type, ms, fs, H5P_DEFAULT,
                              result.data()) >= 0);
                CHECK(result[1].len == 2 && result[3].len == 1 &&
                      result[5].len == 2);
                CHECK(static_cast<int*>(result[1].p)[0] == 8 &&
                      static_cast<int*>(result[1].p)[1] == 9);
                CHECK(static_cast<int*>(result[3].p)[0] == 42);
                CHECK(static_cast<int*>(result[5].p)[0] == 8 &&
                      static_cast<int*>(result[5].p)[1] == 9);
                CHECK(result[1].p != result[5].p);
                for (size_t i : {0, 2, 4, 6})
                    CHECK(result[i].len == 0 && result[i].p == nullptr);
                CHECK(H5Treclaim(type, ms, H5P_DEFAULT, result.data()) >= 0);
            }
            else if (name == "empty-string")
            {
                test::Id type(H5Tcopy(H5T_C_S1), H5Tclose);
                CHECK(H5Tset_size(type, H5T_VARIABLE) >= 0);
                std::array<char*, 2> result {};
                CHECK(H5Dread(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                              result.data()) >= 0);
                CHECK(result[0] != nullptr && result[0][0] == '\0');
                CHECK(result[1] != nullptr &&
                      std::string(result[1]) == "written");
                auto extent = test::space(2);
                CHECK(H5Treclaim(type, extent, H5P_DEFAULT,
                                 result.data()) >= 0);
            }
            else if (name == "fill-string")
            {
                test::Id type(H5Tcopy(H5T_C_S1), H5Tclose);
                CHECK(H5Tset_size(type, H5T_VARIABLE) >= 0);
                std::array<char*, 3> result {};
                CHECK(H5Dread(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                              result.data()) >= 0);
                CHECK(std::string(result[0]) == "unwritten" &&
                      std::string(result[1]) == "written" &&
                      std::string(result[2]) == "unwritten");
                CHECK(result[0] != result[2]);
                auto extent = test::space(3);
                CHECK(H5Treclaim(type, extent, H5P_DEFAULT,
                                 result.data()) >= 0);
            }
            else if (name == "dummy-real" || name == "dummy-copy-whole")
            {
                auto* current = vol.files.at(first);
                CHECK(dynamic_cast<LowFive::File*>(current));
                CHECK(dummy_wrapper->mdata_obj != current);
                CHECK(static_cast<LowFive::Object*>(
                          dummy_wrapper->mdata_obj)->name == first);
                CHECK(H5Iis_valid(dummy_dcpl) > 0);
                test::Id again(H5Fopen(first.c_str(), H5F_ACC_RDONLY, fapl),
                               H5Fclose);
                test::Id other(H5Dopen2(again, "values", H5P_DEFAULT), H5Dclose);
                read_all(ds);
                read_all(other);
                CHECK(vol.MetadataVOL::file_close(
                          dummy_wrapper, H5P_DEFAULT, nullptr) >= 0);
                vol.drop(dummy_wrapper);
                CHECK(H5Iis_valid(dummy_dcpl) == 0);
                CHECK(vol.files.at(first) == current);
                read_all(other);
            }
            else if (name == "scalar")
            {
                int result = -1;
                CHECK(H5Dread(ds, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                              H5P_DEFAULT, &result) >= 0);
                CHECK(result == 37);
            }
            else if (name == "vlen")
            {
                test::Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
                hvl_t result {};
                CHECK(H5Dread(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                              &result) >= 0);
                CHECK(result.len == 3);
                const auto* values = static_cast<const int*>(result.p);
                CHECK(values[0] == 4 && values[1] == 5 && values[2] == 6);
                test::Id space(H5Dget_space(ds), H5Sclose);
                CHECK(H5Treclaim(type, space, H5P_DEFAULT, &result) >= 0);
            }
            else if (name == "vlen-compound")
            {
                auto type = vlen_record_type();
                VLenRecord result {-1, hvl_t {}};
                CHECK(H5Dread(ds, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                              &result) >= 0);
                CHECK(result.tag == 42 && result.values.len == 3);
                const auto* values =
                    static_cast<const int*>(result.values.p);
                CHECK(values[0] == 7 && values[1] == 8 &&
                      values[2] == 9);
                test::Id space(H5Dget_space(ds), H5Sclose);
                CHECK(H5Treclaim(type, space, H5P_DEFAULT, &result) >= 0);
            }
            else if (name == "vlen-compound-selection")
            {
                auto type = wide_vlen_record_type();
                auto file_space = test::space(2);
                auto memory_space = test::space(3);
                select(memory_space, 0, 2, 2);
                int preserved0[] = {90, 91};
                int preserved1[] = {92};
                int preserved2[] = {93, 94, 95};
                std::array<WideVLenRecord, 3> result {{
                    {-1, hvl_t {}, 100, hvl_t {2, preserved0}},
                    {-1, hvl_t {}, 200, hvl_t {1, preserved1}},
                    {-1, hvl_t {}, 300, hvl_t {3, preserved2}}
                }};
                CHECK(H5Dread(ds, type, memory_space, file_space,
                              H5P_DEFAULT, result.data()) >= 0);
                CHECK(result[0].tag == 11 && result[0].values.len == 1);
                CHECK(static_cast<const int*>(
                          result[0].values.p)[0] == 6);
                CHECK(result[0].sentinel == 100);
                CHECK(result[0].preserved.p == preserved0);
                CHECK(result[1].tag == -1 &&
                      result[1].values.p == nullptr);
                CHECK(result[1].sentinel == 200);
                CHECK(result[1].preserved.p == preserved1);
                CHECK(result[2].tag == 20 && result[2].values.len == 3);
                const auto* values =
                    static_cast<const int*>(result[2].values.p);
                CHECK(values[0] == 3 && values[1] == 4 &&
                      values[2] == 5);
                CHECK(result[2].sentinel == 300);
                CHECK(result[2].preserved.p == preserved2);
                CHECK(H5free_memory(result[0].values.p) >= 0);
                CHECK(H5free_memory(result[2].values.p) >= 0);
            }
            else if (name == "repeated-open" || name == "object-open")
            {
                test::Id again(name == "object-open" ? H5Oopen(file, "values", H5P_DEFAULT) :
                                                      H5Dopen2(file, "values", H5P_DEFAULT),
                               name == "object-open" ? H5Oclose : H5Dclose);
                read_all(ds);
                read_all(again);
                ds.reset();
                read_all(again);
                file.reset();
                read_all(again);
            }
            else if (name == "conversion")
            {
                std::array<float, 10> result;
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, result.data() + 1) >= 0);
                CHECK(result.front() == -91);
                for (size_t i = 0; i < expected.size(); ++i) CHECK(result[i + 1] == float(expected[i]));
                for (size_t i = 5; i < result.size(); ++i) CHECK(result[i] == -91);
                test::Id fs(H5Dget_space(ds), H5Sclose);
                select(fs, 1, 2, 2);
                auto ms = test::space(8);
                select(ms, 2, 2, 3);
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_FLOAT, ms, fs, H5P_DEFAULT, result.data() + 1) >= 0);
                for (size_t i = 0; i < result.size(); ++i)
                    CHECK(result[i] == (i == 3 ? float(expected[1]) : i == 6 ? float(expected[3]) : -91));
            }
            else if (name == "memory-all")
            {
                test::Id fs(H5Dget_space(ds), H5Sclose);
                select(fs, 1, 2);
                std::array<double, 6> result;
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, H5S_ALL, fs, H5P_DEFAULT, result.data() + 1) >= 0);
                CHECK(result[0] == -91 && result[1] == -91 && result[5] == -91);
                CHECK(result[2] == expected[1] && result[3] == expected[2] && result[4] == -91);
            }
            else if (name == "irregular")
            {
                test::Id fs(H5Dget_space(ds), H5Sclose);
                select(fs, 1, 2, 2);
                auto ms = test::space(2);
                std::array<double, 4> result = {{-91, -91, -91, -91}};
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, ms, fs, H5P_DEFAULT, result.data() + 1) >= 0);
                CHECK(result[0] == -91 && result[3] == -91);
                CHECK(result[1] == expected[1] && result[2] == expected[3]);
                select(fs, 0, 1);
                const hsize_t start = 2, count = 2;
                CHECK(H5Sselect_hyperslab(fs, H5S_SELECT_OR, &start, nullptr, &count, nullptr) >= 0);
                CHECK(H5Sis_regular_hyperslab(fs) == 0);
                auto packed = test::space(3);
                std::array<double, 5> irregular = {{-91, -91, -91, -91, -91}};
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, packed, fs, H5P_DEFAULT, irregular.data() + 1) >= 0);
                CHECK(irregular[0] == -91 && irregular[4] == -91);
                CHECK(irregular[1] == expected[0] && irregular[2] == expected[2] && irregular[3] == expected[3]);
            }
            else if (name == "bounds")
            {
                test::Id fs(H5Dget_space(ds), H5Sclose);
                auto ms = test::space(2);
                std::array<double, 6> result;
                result.fill(-91);
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, ms, fs, H5P_DEFAULT, result.data() + 1) < 0);
                CHECK(std::all_of(result.begin(), result.end(), [](double x) { return x == -91; }));
                select(fs, 4, 1);
                auto one = test::space(1);
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, one, fs, H5P_DEFAULT, result.data() + 1) < 0);
                CHECK(std::all_of(result.begin(), result.end(), [](double x) { return x == -91; }));
            }
            else if (name == "empty")
            {
                test::Id fs(H5Dget_space(ds), H5Sclose);
                auto ms = test::space(4);
                CHECK(H5Sselect_none(fs) >= 0 && H5Sselect_none(ms) >= 0);
                double guard = -91;
                CHECK(H5Dread(ds, H5T_NATIVE_DOUBLE, ms, fs, H5P_DEFAULT, &guard) >= 0);
                CHECK(guard == -91);
            }
            else if (name == "missing")
            {
                CHECK(H5Dopen2(file, "missing", H5P_DEFAULT) < 0);
                CHECK(H5Oopen(file, "missing", H5P_DEFAULT) < 0);
                read_all(ds);
            }
            else if (name == "multifile")
            {
                test::Id other(H5Fopen(second.c_str(), H5F_ACC_RDONLY, fapl), H5Fclose);
                test::Id other_ds(H5Dopen2(other, "values", H5P_DEFAULT), H5Dclose);
                ds.reset();
                file.reset();
                read_all(other_ds);
            }
            else if (name == "hard-link-replace")
            {
                test::Id alias(H5Dopen2(file, "alias", H5P_DEFAULT), H5Dclose);
                read_all(ds);
                read_all(alias);
            }
            else if (name == "remote-unlinked-alias")
            {
                CHECK(H5Olink(ds, file, "alias", H5P_DEFAULT, H5P_DEFAULT) >= 0);
                CHECK(H5Ldelete(file, "values", H5P_DEFAULT) >= 0);
                ds.reset();
                test::Id alias(H5Dopen2(file, "alias", H5P_DEFAULT), H5Dclose);
                read_all(alias);
                alias.reset();
                file.reset();
            }
            else if (name == "hard-link-parent-relative")
            {
                test::Id group(H5Gopen2(file, "alias", H5P_DEFAULT),
                               H5Gclose);
                test::Id nested(H5Dopen2(group, "d", H5P_DEFAULT), H5Dclose);
                read_all(nested);
            }
            else read_all(ds);
        }
        CHECK(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
    }
    fapl.reset();
    CHECK(H5VLunregister_connector(connector) >= 0);
    if (key != MPI_KEYVAL_INVALID)
    {
        if (copies != 1)
            std::fprintf(stderr, "rank %d: %d MPI communicator references remain; expected original only\n", rank, copies);
        CHECK(copies == 1);
        CHECK(MPI_Comm_delete_attr(local, key) == MPI_SUCCESS);
        CHECK(copies == 0);
        CHECK(MPI_Comm_free_keyval(&key) == MPI_SUCCESS);
    }
}
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int rank = 0, size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    try
    {
        const char* selected = nullptr;
        if (argc == 2)
            selected = argv[1];
        else if (argc == 3 &&
                 std::string(argv[1]) == "--lowfive-mpi-postflag")
            selected = argv[2];
        CHECK(selected);
        const std::string name = selected;
        const bool grouped_consumers =
            name == "copy-whole" || name == "dummy-copy-whole";
        CHECK(size == (grouped_consumers ? 3 : 2));
        H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
        if (name == "intercomm-indices")
        {
            exercise_intercomm_indices();
            MPI_Finalize();
            return EXIT_SUCCESS;
        }
        if (name == "broadcast-replace")
        {
            CHECK(size == 2);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
            exercise_broadcast(rank);
            MPI_Finalize();
            return EXIT_SUCCESS;
        }
        MPI_Comm local, intercomm;
        const int color = grouped_consumers && rank > 0 ? 1 : rank;
        CHECK(MPI_Comm_split(MPI_COMM_WORLD, color, rank, &local) ==
              MPI_SUCCESS);
        const int remote_leader = color == 0 ? 1 : 0;
        CHECK(MPI_Intercomm_create(local, 0, MPI_COMM_WORLD, remote_leader,
                                   17, &intercomm) == MPI_SUCCESS);
        if (name == "serving-cleanup")
            exercise_serving_cleanup(rank, local, intercomm);
        else
            exercise(name == "postflag-order" ? "contents" : name,
                     rank, local, intercomm);
        CHECK(MPI_Comm_free(&intercomm) == MPI_SUCCESS);
        CHECK(MPI_Comm_free(&local) == MPI_SUCCESS);
        MPI_Finalize();
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::fprintf(stderr, "rank %d: %s\n", rank, error.what());
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }
}
