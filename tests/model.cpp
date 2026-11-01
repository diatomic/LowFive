#include "test.hpp"
#include "vol-metadata-private.hpp"
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>

namespace
{
using test::Id;
Id string_type()
{
    Id type(H5Tcopy(H5T_C_S1), H5Tclose);
    CHECK(H5Tset_size(type, H5T_VARIABLE) >= 0);
    return type;
}
void select(hid_t space, hsize_t start, hsize_t count)
{
    CHECK(H5Sselect_hyperslab(space, H5S_SELECT_SET, &start, nullptr, &count, nullptr) >= 0);
}
void attribute_conversion()
{
    test::Connector c("model-attribute-conversion"); auto f = c.file(); auto s = test::space(2);
    Id a(H5Acreate2(f, "values", H5T_NATIVE_DOUBLE, s, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    double input[] = {12.75, -8.5}; CHECK(H5Awrite(a, H5T_NATIVE_DOUBLE, input) >= 0);
    struct { int values[2]; unsigned guard[2]; } out{{0, 0}, {0x12345678, 0x87654321}};
    CHECK(H5Aread(a, H5T_NATIVE_INT, out.values) >= 0);
    CHECK(out.values[0] == 12 && out.values[1] == -8);
    CHECK(out.guard[0] == 0x12345678 && out.guard[1] == 0x87654321);
    double same[2]{}; CHECK(H5Aread(a, H5T_NATIVE_DOUBLE, same) >= 0);
    CHECK(same[0] == input[0] && same[1] == input[1]);
    int replacement[] = {31, -22}; CHECK(H5Awrite(a, H5T_NATIVE_INT, replacement) >= 0);
    CHECK(H5Aread(a, H5T_NATIVE_DOUBLE, same) >= 0); CHECK(same[0] == 31 && same[1] == -22);
}
void attribute_compound()
{
    test::Connector c("model-attribute-compound"); auto f = c.file(); auto s = test::space(3);
    struct Record { int number; double value; };
    Id type(H5Tcreate(H5T_COMPOUND, sizeof(Record)), H5Tclose);
    CHECK(H5Tinsert(type, "number", HOFFSET(Record, number), H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(type, "value", HOFFSET(Record, value), H5T_NATIVE_DOUBLE) >= 0);
    Id a(H5Acreate2(f, "records", type, s, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    Record input[] = {{1, 2.5}, {3, 4.5}, {5, -6.5}}, out[3]{};
    CHECK(H5Awrite(a, type, input) >= 0); CHECK(H5Aread(a, type, out) >= 0);
    for (int i = 0; i < 3; ++i) CHECK(out[i].number == input[i].number && out[i].value == input[i].value);
    LowFive::Attribute local("records", type, s); local.write(LowFive::Datatype(type), input);
    diy::MemoryBuffer bb; auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    LowFive::serialize(bb, &local, vol); bb.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(bb, vol));
    std::memset(out, 0, sizeof out); static_cast<LowFive::Attribute*>(copy.get())->read(LowFive::Datatype(type), out);
    for (int i = 0; i < 3; ++i) CHECK(out[i].number == input[i].number && out[i].value == input[i].value);
}
void attribute_strings()
{
    test::Connector c("model-attribute-strings"); auto f = c.file(); auto s = test::space(3); auto type = string_type();
    Id a(H5Acreate2(f, "strings", type, s, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    const char* old[] = {"old", "previous", "value"}; const char* input[] = {"new", "", "last"}; char* out[3]{};
    CHECK(H5Awrite(a, type, old) >= 0); CHECK(H5Awrite(a, type, input) >= 0); CHECK(H5Aread(a, type, out) >= 0);
    for (int i = 0; i < 3; ++i) CHECK(std::string(out[i] ? out[i] : "") == input[i]);
    CHECK(H5Treclaim(type, s, H5P_DEFAULT, out) >= 0);
}
void dataset_conversion()
{
    test::Connector c("model-dataset-conversion"); auto f = c.file(); auto s = test::space(4);
    Id d(H5Dcreate2(f, "values", H5T_NATIVE_DOUBLE, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    float input[] = {1.5f, -2.5f, 3.25f, 4.5f}; double out[4]{};
    CHECK(H5Dwrite(d, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, input) >= 0);
    CHECK(H5Dread(d, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, out) >= 0);
    for (int i = 0; i < 4; ++i) CHECK(out[i] == input[i]);
    int ints[4]{}; CHECK(H5Dread(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, ints) >= 0);
    CHECK(ints[0] == 1 && ints[1] == -2 && ints[2] == 3 && ints[3] == 4);
    Id integral(H5Dcreate2(f, "integral", H5T_NATIVE_INT, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    CHECK(H5Dwrite(integral, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, input) >= 0);
    CHECK(H5Dread(integral, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, out) >= 0);
    CHECK(out[0] == 1 && out[1] == -2 && out[2] == 3 && out[3] == 4);
}
void dataset_all(bool writing)
{
    test::Connector c(writing ? "model-write-all" : "model-read-all"); auto f = c.file(); auto s = test::space(4);
    Id d(H5Dcreate2(f, "values", H5T_NATIVE_INT, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int initial[] = {10, 20, 30, 40}; CHECK(H5Dwrite(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, initial) >= 0);
    select(s, 1, 2); int out[] = {-1, -1, -1, -1};
    if (writing)
    {
        int update[] = {99, 21, 31, 99}; CHECK(H5Dwrite(d, H5T_NATIVE_INT, H5S_ALL, s, H5P_DEFAULT, update) >= 0);
        CHECK(H5Dread(d, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, out) >= 0);
        CHECK(out[0] == 10 && out[1] == 21 && out[2] == 31 && out[3] == 40);
    }
    else
    {
        CHECK(H5Dread(d, H5T_NATIVE_INT, H5S_ALL, s, H5P_DEFAULT, out) >= 0);
        CHECK(out[0] == -1 && out[1] == 20 && out[2] == 30 && out[3] == -1);
    }
}
void dataset_strings(bool zerocopy)
{
    test::Connector c(zerocopy ? "model-zerocopy-strings" : "model-selected-strings");
    if (zerocopy) dynamic_cast<LowFive::MetadataVOL&>(c.vol).set_zerocopy(c.filename, "*");
    auto f = c.file(); auto s = test::space(2); auto type = string_type();
    Id d(H5Dcreate2(f, "strings", type, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    const char* input[] = {"selected", nullptr}; auto fs = test::space(2); auto ms = test::space(2);
    select(fs, 1, 1); select(ms, 0, 1);
    CHECK(H5Dwrite(d, type, ms, fs, H5P_DEFAULT, input) >= 0);
    input[0] = "changed"; auto one = test::space(1); char* out = nullptr;
    CHECK(H5Dread(d, type, one, fs, H5P_DEFAULT, &out) >= 0); CHECK(out && std::string(out) == "selected");
    CHECK(H5Treclaim(type, one, H5P_DEFAULT, &out) >= 0);
}
void datatype_ownership()
{
    hid_t raw = H5Tcopy(H5T_NATIVE_INT); CHECK(raw >= 0);
    { LowFive::Datatype owned(raw, true); CHECK(H5Iis_valid(raw) > 0); }
    CHECK(H5Iis_valid(raw) == 0);
}
void persistence(bool strings)
{
    auto s = test::space(2); auto type = string_type();
    LowFive::File f("source", H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
    auto* d = new LowFive::Dataset("values", strings ? type.id : H5T_NATIVE_INT, s, LowFive::Dataset::lowfive,
                                 H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT, false, true);
    f.add_child(d);
    auto* a = new LowFive::Attribute("description", type, s); d->add_child(a);
    const char* text[] = {"first", "second"}; a->write(LowFive::Datatype(type), text);
    int values[] = {3, 9}; d->write(LowFive::Datatype(strings ? type.id : H5T_NATIVE_INT), LowFive::Dataspace(s), LowFive::Dataspace(s), strings ? static_cast<const void*>(text) : values);
    const char* filename = strings ? "model-persist-strings.h5" : "model-persist-attributes.h5";
    LowFive::save(f, filename);
    Id file(H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id dataset(H5Dopen2(file, "values", H5P_DEFAULT), H5Dclose);
    if (strings)
    {
        char* out[2]{}; CHECK(H5Dread(dataset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, out) >= 0);
        CHECK(out[0] && out[1] && std::string(out[0]) == text[0] && std::string(out[1]) == text[1]);
        CHECK(H5Treclaim(type, s, H5P_DEFAULT, out) >= 0);
    }
    Id attr(H5Aopen(dataset, "description", H5P_DEFAULT), H5Aclose); char* out[2]{};
    CHECK(H5Aread(attr, type, out) >= 0); CHECK(out[0] && out[1] && std::string(out[0]) == text[0] && std::string(out[1]) == text[1]);
    CHECK(H5Treclaim(type, s, H5P_DEFAULT, out) >= 0); std::remove(filename);
}
void vlen_rewrite()
{
    test::Connector c("model-vlen-rewrite"); auto f = c.file(); auto s = test::space(2);
    Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    Id a(H5Acreate2(f, "arrays", type, s, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    int first[] = {1, 2, 3}, second[] = {8, 9}; hvl_t input[] = {{3, first}, {0, nullptr}};
    CHECK(H5Awrite(a, type, input) >= 0); input[0] = {2, second}; CHECK(H5Awrite(a, type, input) >= 0);
    hvl_t out[2]{}; CHECK(H5Aread(a, type, out) >= 0);
    CHECK(out[0].len == 2 && static_cast<int*>(out[0].p)[0] == 8 && static_cast<int*>(out[0].p)[1] == 9 && out[1].len == 0);
    CHECK(H5Treclaim(type, s, H5P_DEFAULT, out) >= 0);
}
void dataset_vlen_overlap()
{
    test::Connector c("model-dataset-vlen-overlap");
    auto file = c.file();
    auto space = test::scalar();
    Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    Id dataset(H5Dcreate2(file, "values", type, space, H5P_DEFAULT,
                          H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int first_values[] = {1, 2, 3};
    int second_values[] = {8, 9};
    hvl_t first{3, first_values};
    hvl_t second{2, second_values};
    CHECK(H5Dwrite(dataset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &first) >= 0);
    CHECK(H5Dwrite(dataset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &second) >= 0);
    hvl_t output{};
    CHECK(H5Dread(dataset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &output) >= 0);
    CHECK(output.len == 2);
    CHECK(static_cast<int*>(output.p)[0] == 8 && static_cast<int*>(output.p)[1] == 9);
    CHECK(H5Treclaim(type, space, H5P_DEFAULT, &output) >= 0);
}
void dataset_vlen_serialization()
{
    test::Connector c("model-dataset-vlen-serialization");
    auto space = test::space(2);
    Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
    source.copy_whole = true;
    auto* dataset = new LowFive::Dataset(
        "values", type, space, LowFive::Dataset::lowfive,
        H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT, false, true);
    source.add_child(dataset);
    int first[] = {1, 2, 3}, second[] = {8, 9};
    hvl_t input[] = {{3, first}, {2, second}};
    dataset->write(LowFive::Datatype(type), LowFive::Dataspace(space),
                   LowFive::Dataspace(space), input);

    diy::MemoryBuffer buffer;
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    LowFive::serialize(buffer, &source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* copied = dynamic_cast<LowFive::Dataset*>(copy->search("values").exact());
    CHECK(copied && copied->data.size() == 1);
    auto* source_values = reinterpret_cast<const hvl_t*>(dataset->data.front().data);
    auto* copied_values = reinterpret_cast<hvl_t*>(copied->data.front().owned_data.get());
    const bool independent = copied_values[0].p != source_values[0].p &&
                             copied_values[1].p != source_values[1].p;
    if (!independent)
    {
        for (size_t i = 0; i < 2; ++i)
            if (copied_values[i].p == source_values[i].p)
                copied_values[i] = {0, nullptr};
    }
    CHECK(independent);

    hvl_t output[2]{};
    copied->read(type, H5S_ALL, H5S_ALL, output);
    CHECK(output[0].len == 3 && output[1].len == 2);
    CHECK(static_cast<int*>(output[0].p)[0] == 1 &&
          static_cast<int*>(output[0].p)[2] == 3 &&
          static_cast<int*>(output[1].p)[0] == 8 &&
          static_cast<int*>(output[1].p)[1] == 9);
    CHECK(H5Treclaim(type, space, H5P_DEFAULT, output) >= 0);
}
void dataset_vlen_sparse_serialization()
{
    test::Connector c("model-dataset-vlen-sparse-serialization");
    auto extent = test::space(3);
    auto memory = test::space(3);
    auto selected_file = test::space(3);
    auto one = test::space(1);
    select(memory, 1, 1);
    select(selected_file, 2, 1);
    Id type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
    source.copy_whole = true;
    auto* dataset = new LowFive::Dataset(
        "values", type, extent, LowFive::Dataset::user,
        H5P_DATASET_CREATE_DEFAULT, H5P_DATASET_ACCESS_DEFAULT, false, true);
    source.add_child(dataset);
    int values[] = {7, 8};
    auto* invalid = reinterpret_cast<void*>(std::uintptr_t{1});
    hvl_t input[] = {{1, invalid}, {2, values}, {1, invalid}};
    dataset->write(LowFive::Datatype(type), LowFive::Dataspace(memory),
                   LowFive::Dataspace(selected_file), input);

    diy::MemoryBuffer buffer;
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    LowFive::serialize(buffer, &source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* copied = dynamic_cast<LowFive::Dataset*>(copy->search("values").exact());
    CHECK(copied);

    hvl_t output{};
    copied->read(type, one, selected_file, &output);
    CHECK(output.len == 2);
    CHECK(static_cast<int*>(output.p)[0] == 7 &&
          static_cast<int*>(output.p)[1] == 8);
    CHECK(H5Treclaim(type, one, H5P_DEFAULT, &output) >= 0);
}
void shutdown_cleanup()
{
    auto* c = new test::Connector("model-shutdown-cleanup");
    auto file = c->file();
    auto space = test::space(1);
    Id dataset(H5Dcreate2(file, "values", H5T_NATIVE_INT, space,
                          H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int value = 7;
    CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value) >= 0);
    Id attribute(H5Acreate2(dataset, "value", H5T_NATIVE_INT, space,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attribute, H5T_NATIVE_INT, &value) >= 0);

    unsigned errors = 0;
    auto count_error = [](hid_t, void* data) -> herr_t
    {
        ++*static_cast<unsigned*>(data);
        return 0;
    };
    CHECK(H5Eset_auto2(H5E_DEFAULT, count_error, &errors) >= 0);
    herr_t status = H5close();
    file.id = dataset.id = attribute.id = space.id = H5I_INVALID_HID;
    CHECK(status >= 0);
    CHECK(errors == 0);
}
void reference_roundtrip(bool compound)
{
    test::Connector c(compound ? "model-compound-reference" : "model-top-reference"); auto f = c.file(); auto s = test::scalar();
    Id d(H5Dcreate2(f, "target", H5T_NATIVE_INT, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t ref{}; CHECK(H5Rcreate_object(f, "target", H5P_DEFAULT, &ref) >= 0);
    Id type(compound ? H5Tcreate(H5T_COMPOUND, sizeof(H5R_ref_t)) : H5Tcopy(H5T_STD_REF), H5Tclose);
    if (compound) CHECK(H5Tinsert(type, "reference", 0, H5T_STD_REF) >= 0);
    Id attr(H5Acreate2(f, "reference", type, s, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attr, type, &ref) >= 0);
    Id references(H5Dcreate2(f, "references", type, s, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    CHECK(H5Dwrite(references, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &ref) >= 0);

    diy::MemoryBuffer bb; auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = vol.files.at(c.filename);
    static_cast<LowFive::File*>(source)->copy_whole = true;
    LowFive::serialize(bb, source, vol); bb.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(bb, vol));
    auto* tree = dynamic_cast<LowFive::File*>(copy.get()); CHECK(tree);
    CHECK(tree->references.size() == 2);
    for (const auto& pending : tree->references)
        CHECK(pending.second.token == source->search("target").exact()->token);

    CHECK(H5Rdestroy(&ref) >= 0);
    attr.reset();
    references.reset();
    d.reset();
    f.reset();
    vol.clear_files();
    CHECK(vol.files.find(c.filename) == vol.files.end());
    CHECK(vol.files.emplace(c.filename, copy.release()).second);

    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDONLY, c.fapl), H5Fclose);
    auto check_pending = [&]()
    {
        CHECK(tree->references.size() == 2);
        for (const auto& pending : tree->references)
            CHECK(pending.second.token == tree->search("target").exact()->token);
    };
    H5R_ref_t output{};
    Id copied_references(H5Dopen2(reopened, "references", H5P_DEFAULT), H5Dclose);
    CHECK(H5Dread(copied_references, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &output) >= 0);
    check_pending();
    CHECK(H5Rget_type(&output) == H5R_OBJECT2);
    CHECK(H5Rdestroy(&output) >= 0);
    Id copied_attribute(H5Aopen(reopened, "reference", H5P_DEFAULT), H5Aclose);
    CHECK(H5Aread(copied_attribute, type, &output) >= 0);
    check_pending();
    CHECK(H5Rget_type(&output) == H5R_OBJECT2);
    CHECK(H5Rdestroy(&output) >= 0);
}
void region_reference_serialization()
{
    test::Connector c("model-region-reference-serialization");
    auto file = c.file();
    auto scalar = test::scalar();
    auto space = test::space(4);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, space, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    select(space, 1, 2);
    H5R_ref_t reference{};
    CHECK(H5Rcreate_region(file, "target", space, H5P_DEFAULT,
                           &reference) >= 0);
    Id attribute(H5Acreate2(file, "region", H5T_STD_REF, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attribute, H5T_STD_REF, &reference) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(c.filename));
    CHECK(source);
    source->copy_whole = true;
    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* tree = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(tree && tree->references.size() == 1);

    CHECK(H5Rdestroy(&reference) >= 0);
    attribute.reset();
    target.reset();
    file.reset();
    vol.clear_files();
    CHECK(vol.files.emplace(c.filename, copy.release()).second);

    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDONLY, c.fapl), H5Fclose);
    Id copied(H5Aopen(reopened, "region", H5P_DEFAULT), H5Aclose);
    H5R_ref_t output{};
    CHECK(H5Aread(copied, H5T_STD_REF, &output) >= 0);
    CHECK(H5Rget_type(&output) == H5R_DATASET_REGION2);
    Id selected(H5Ropen_region(&output, H5P_DEFAULT, H5P_DEFAULT), H5Sclose);
    CHECK(H5Sget_select_npoints(selected) == 2);
    hsize_t start = 0, end = 0;
    CHECK(H5Sget_select_bounds(selected, &start, &end) >= 0);
    CHECK(start == 1 && end == 2);
    CHECK(H5Rdestroy(&output) >= 0);
}
void hard_link_serialization()
{
    test::Connector c("model-hard-link-serialization");
    auto file = c.file();
    auto scalar = test::scalar();
    Id dataset(H5Dcreate2(file, "d", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                          H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int input = 73;
    CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, &input) >= 0);
    CHECK(H5Olink(dataset, file, "alias", H5P_DEFAULT, H5P_DEFAULT) >= 0);
    CHECK(H5Ldelete(file, "d", H5P_DEFAULT) >= 0);
    dataset.reset();

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(c.filename));
    CHECK(source);
    source->copy_whole = true;
    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* copied_file = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(copied_file);
    CHECK(dynamic_cast<LowFive::Dataset*>(copied_file->search("alias").exact()));

    file.reset();
    vol.clear_files();
    CHECK(vol.files.emplace(c.filename, copy.release()).second);
    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDONLY, c.fapl), H5Fclose);
    Id alias(H5Dopen2(reopened, "alias", H5P_DEFAULT), H5Dclose);
    int output = 0;
    CHECK(H5Dread(alias, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, &output) >= 0);
    CHECK(output == input);
}
void pending_reference_cleanup(bool remove)
{
    test::Connector c(remove ? "model-reference-delete" :
                               "model-reference-overwrite");
    auto file = c.file();
    auto scalar = test::scalar();
    Id original(H5Dcreate2(file, "original", H5T_NATIVE_INT, scalar,
                            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id replacement(H5Dcreate2(file, "replacement", H5T_NATIVE_INT, scalar,
                               H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t original_reference{};
    CHECK(H5Rcreate_object(file, "original", H5P_DEFAULT,
                           &original_reference) >= 0);
    Id type(H5Tvlen_create(H5T_STD_REF), H5Tclose);
    hvl_t input {1, &original_reference};
    Id attribute(H5Acreate2(file, "reference", type, scalar, H5P_DEFAULT,
                            H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attribute, type, &input) >= 0);
    Id sentinel(H5Acreate2(file, "sentinel", H5T_NATIVE_INT, scalar,
                           H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    int sentinel_value = 19;
    CHECK(H5Awrite(sentinel, H5T_NATIVE_INT, &sentinel_value) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(c.filename));
    CHECK(source);
    source->copy_whole = true;
    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* tree = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(tree && tree->references.size() == 1);

    CHECK(H5Rdestroy(&original_reference) >= 0);
    attribute.reset();
    sentinel.reset();
    original.reset();
    replacement.reset();
    file.reset();
    CHECK(vol.our_objects.empty());
    vol.clear_files();
    CHECK(vol.files.emplace(c.filename, copy.release()).second);

    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDWR, c.fapl), H5Fclose);
    Id copied_attribute(H5Aopen(reopened, "reference", H5P_DEFAULT), H5Aclose);
    if (remove)
    {
        copied_attribute.reset();
        CHECK(H5Adelete(reopened, "reference") >= 0);
        CHECK(tree->references.empty());
        Id copied_sentinel(H5Aopen(reopened, "sentinel", H5P_DEFAULT), H5Aclose);
        int output = 0;
        CHECK(H5Aread(copied_sentinel, H5T_NATIVE_INT, &output) >= 0);
        CHECK(output == sentinel_value);
        copied_sentinel.reset();
        reopened.reset();
        vol.clear_files();
        return;
    }

    H5R_ref_t replacement_reference{};
    CHECK(H5Rcreate_object(reopened, "replacement", H5P_DEFAULT,
                           &replacement_reference) >= 0);
    hvl_t replacement_input {1, &replacement_reference};
    CHECK(H5Awrite(copied_attribute, type, &replacement_input) >= 0);
    CHECK(tree->references.size() == 1);
    hvl_t output{};
    CHECK(H5Aread(copied_attribute, type, &output) >= 0);
    CHECK(output.len == 1);
    Id opened(H5Ropen_object(static_cast<H5R_ref_t*>(output.p),
                             H5P_DEFAULT, H5P_DEFAULT), H5Oclose);
    std::array<char, 32> name{};
    CHECK(H5Iget_name(opened, name.data(), name.size()) == 12);
    CHECK(std::string(name.data()) == "/replacement");
    CHECK(H5Treclaim(type, scalar, H5P_DEFAULT, &output) >= 0);
    CHECK(tree->references.size() == 1);
    hvl_t repeated{};
    CHECK(H5Aread(copied_attribute, type, &repeated) >= 0);
    CHECK(repeated.len == 1);
    Id repeated_opened(H5Ropen_object(static_cast<H5R_ref_t*>(repeated.p),
                                      H5P_DEFAULT, H5P_DEFAULT), H5Oclose);
    std::array<char, 32> repeated_name{};
    CHECK(H5Iget_name(repeated_opened, repeated_name.data(),
                      repeated_name.size()) == 12);
    CHECK(std::string(repeated_name.data()) == "/replacement");
    CHECK(H5Treclaim(type, scalar, H5P_DEFAULT, &repeated) >= 0);
    CHECK(tree->references.size() == 1);
    CHECK(H5Rdestroy(&replacement_reference) >= 0);
    opened.reset();
    copied_attribute.reset();
    reopened.reset();
    vol.clear_files();
}
void pending_dataset_reference_cleanup()
{
    test::Connector c("model-dataset-reference-delete");
    auto file = c.file();
    auto scalar = test::scalar();
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t reference{};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
    Id type(H5Tvlen_create(H5T_STD_REF), H5Tclose);
    Id references(H5Dcreate2(file, "references", type, scalar, H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    hvl_t input {1, &reference};
    CHECK(H5Dwrite(references, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &input) >= 0);
    Id sentinel(H5Dcreate2(file, "sentinel", H5T_NATIVE_INT, scalar,
                           H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int sentinel_value = 29;
    CHECK(H5Dwrite(sentinel, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, &sentinel_value) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(c.filename));
    CHECK(source);
    source->copy_whole = true;
    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* tree = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(tree && tree->references.size() == 1);

    CHECK(H5Rdestroy(&reference) >= 0);
    references.reset();
    sentinel.reset();
    target.reset();
    file.reset();
    CHECK(vol.our_objects.empty());
    vol.clear_files();
    CHECK(vol.files.emplace(c.filename, copy.release()).second);

    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDWR, c.fapl), H5Fclose);
    CHECK(H5Ldelete(reopened, "references", H5P_DEFAULT) >= 0);
    CHECK(tree->references.empty());
    Id copied_sentinel(H5Dopen2(reopened, "sentinel", H5P_DEFAULT), H5Dclose);
    int output = 0;
    CHECK(H5Dread(copied_sentinel, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, &output) >= 0);
    CHECK(output == sentinel_value);
    copied_sentinel.reset();
    reopened.reset();
    vol.clear_files();
}
void persistence_references()
{
    test::Connector connector("model-persist-references");
    auto file = connector.file();
    auto target_space = test::space(4);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, target_space,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    const int values[] = {3, 5, 7, 11};
    CHECK(H5Dwrite(target, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   values) >= 0);

    auto reference_space = test::space(2);
    Id references(H5Dcreate2(file, "references", H5T_STD_REF, reference_space,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    std::array<H5R_ref_t, 2> input {};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &input[0]) >= 0);
    select(target_space, 1, 2);
    CHECK(H5Rcreate_region(file, "target", target_space, H5P_DEFAULT,
                           &input[1]) >= 0);
    CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   input.data()) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(connector.filename));
    CHECK(source && source->references.size() == input.size());
    const char* saved_name = "model-persist-references-output.h5";
    LowFive::save(*source, saved_name);

    Id saved(H5Fopen(saved_name, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id saved_references(H5Dopen2(saved, "references", H5P_DEFAULT), H5Dclose);
    std::array<H5R_ref_t, 2> output {};
    CHECK(H5Dread(saved_references, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  output.data()) >= 0);
    CHECK(H5Rget_type(&output[0]) == H5R_OBJECT2);
    CHECK(H5Rget_type(&output[1]) == H5R_DATASET_REGION2);
    Id opened(H5Ropen_object(&output[0], H5P_DEFAULT, H5P_DEFAULT), H5Oclose);
    std::array<int, 4> persisted {};
    CHECK(H5Dread(opened, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  persisted.data()) >= 0);
    const std::array<int, 4> expected {3, 5, 7, 11};
    CHECK(persisted == expected);
    Id selected(H5Ropen_region(&output[1], H5P_DEFAULT, H5P_DEFAULT), H5Sclose);
    CHECK(H5Sget_select_npoints(selected) == 2);
    hsize_t start = 0;
    hsize_t end = 0;
    CHECK(H5Sget_select_bounds(selected, &start, &end) >= 0);
    CHECK(start == 1 && end == 2);
    for (auto& reference : output) CHECK(H5Rdestroy(&reference) >= 0);
    for (auto& reference : input) CHECK(H5Rdestroy(&reference) >= 0);
    std::remove(saved_name);
}
void persistence_cross_file_reference()
{
    test::Connector connector("model-persist-cross-file-reference");
    auto file = connector.file();
    auto scalar = test::scalar();
    Id local(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                        H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int local_value = 17;
    CHECK(H5Dwrite(local, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &local_value) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    const std::string target_name = connector.filename + ".target";
    vol.set_memory(target_name, "*");
    Id target_file(H5Fcreate(target_name.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT,
                             connector.fapl), H5Fclose);
    Id target(H5Dcreate2(target_file, "target", H5T_NATIVE_INT, scalar,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int target_value = 43;
    CHECK(H5Dwrite(target, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &target_value) >= 0);
    H5R_ref_t reference {};
    CHECK(H5Rcreate_object(target_file, "target", H5P_DEFAULT, &reference) >=
          0);
    Id attribute(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attribute, H5T_STD_REF, &reference) >= 0);
    CHECK(H5Rdestroy(&reference) >= 0);

    auto* source = dynamic_cast<LowFive::File*>(
        vol.files.at(connector.filename));
    CHECK(source);
    const char* saved_name = "model-persist-cross-file-reference-output.h5";
    bool rejected = false;
    try
    {
        LowFive::save(*source, saved_name);
    }
    catch (const LowFive::MetadataError&)
    {
        rejected = true;
    }
    CHECK(rejected);
    std::remove(saved_name);
    std::remove(target_name.c_str());
}
void persistence_overwritten_reference()
{
    test::Connector connector("model-persist-overwritten-reference");
    auto file = connector.file();
    auto scalar = test::scalar();
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id references(H5Dcreate2(file, "references", H5T_STD_REF, scalar,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t reference {};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
    CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &reference) >= 0);
    CHECK(H5Rdestroy(&reference) >= 0);
    H5R_ref_t empty {};
    CHECK(H5Dwrite(references, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &empty) >= 0);
    target.reset();
    CHECK(H5Ldelete(file, "target", H5P_DEFAULT) >= 0);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    auto* source = dynamic_cast<LowFive::File*>(
        vol.files.at(connector.filename));
    CHECK(source);
    const char* saved_name =
        "model-persist-overwritten-reference-output.h5";
    LowFive::save(*source, saved_name);
    Id saved(H5Fopen(saved_name, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id saved_references(
        H5Dopen2(saved, "references", H5P_DEFAULT), H5Dclose);
    H5R_ref_t output {};
    CHECK(H5Dread(saved_references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, &output) >= 0);
    CHECK(std::memcmp(&output, &empty, sizeof(output)) == 0);
    std::remove(saved_name);
}
void persistence_deleted_reference_path_reuse()
{
    test::Connector connector("model-persist-deleted-reference-path-reuse");
    auto file = connector.file();
    auto scalar = test::scalar();
    Id original(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                           H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t reference {};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
    Id holder(H5Acreate2(file, "reference", H5T_STD_REF, scalar,
                         H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(holder, H5T_STD_REF, &reference) >= 0);
    CHECK(H5Rdestroy(&reference) >= 0);
    original.reset();
    CHECK(H5Ldelete(file, "target", H5P_DEFAULT) >= 0);
    Id replacement(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar,
                              H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT),
                   H5Dclose);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    auto* source = dynamic_cast<LowFive::File*>(
        vol.files.at(connector.filename));
    CHECK(source);
    const char* saved_name =
        "model-persist-deleted-reference-path-reuse-output.h5";
    bool rejected = false;
    try
    {
        LowFive::save(*source, saved_name);
    }
    catch (const LowFive::MetadataError&)
    {
        rejected = true;
    }
    CHECK(rejected);
    std::remove(saved_name);
}
void persistence_selected_vlen_reference()
{
    test::Connector connector("model-persist-selected-vlen-reference");
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    vol.set_zerocopy(connector.filename, "/references");
    auto file = connector.file();
    auto extent = test::space(3);
    auto memory = test::space(3);
    auto selected_file = test::space(3);
    select(memory, 1, 1);
    select(selected_file, 2, 1);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, test::scalar(),
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id type(H5Tvlen_create(H5T_STD_REF), H5Tclose);
    Id references(H5Dcreate2(file, "references", type, extent, H5P_DEFAULT,
                             H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t reference {};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
    auto* invalid = reinterpret_cast<void*>(std::uintptr_t {1});
    std::array<hvl_t, 3> input {{
        {1, invalid}, {1, &reference}, {1, invalid}
    }};
    CHECK(H5Dwrite(references, type, memory, selected_file, H5P_DEFAULT,
                   input.data()) >= 0);

    auto* source = dynamic_cast<LowFive::File*>(
        vol.files.at(connector.filename));
    auto* target_metadata = source ? source->search("target").exact() : nullptr;
    CHECK(source && target_metadata);
    LowFive::File::PendingReference state;
    state.type = H5R_OBJECT2;
    state.filename = connector.filename;
    state.token = target_metadata->token;
    state.path = "/target";
    source->references.emplace(&reference, std::move(state));
    CHECK(H5Rdestroy(&reference) >= 0);
    std::memset(&reference, 0, sizeof(reference));

    const char* saved_name =
        "model-persist-selected-vlen-reference-output.h5";
    LowFive::save(*source, saved_name);
    Id saved(H5Fopen(saved_name, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id saved_references(
        H5Dopen2(saved, "references", H5P_DEFAULT), H5Dclose);
    std::array<hvl_t, 3> output {};
    CHECK(H5Dread(saved_references, type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  output.data()) >= 0);
    CHECK(output[0].len == 0 && output[1].len == 0 && output[2].len == 1);
    auto* saved_reference = static_cast<H5R_ref_t*>(output[2].p);
    CHECK(saved_reference);
    Id opened(H5Ropen_object(saved_reference, H5P_DEFAULT, H5P_DEFAULT),
              H5Oclose);
    CHECK(opened.id >= 0);
    CHECK(H5Rdestroy(saved_reference) >= 0);
    std::memset(saved_reference, 0, sizeof(*saved_reference));
    CHECK(H5Treclaim(type, extent, H5P_DEFAULT, output.data()) >= 0);
    std::remove(saved_name);
}
void persistence_unwritten_attribute()
{
    auto scalar = test::scalar();
    LowFive::File file(
        "model-persist-unwritten-attribute", H5P_FILE_CREATE_DEFAULT,
        H5P_FILE_ACCESS_DEFAULT);
    file.add_child(new LowFive::Attribute(
        "unwritten", H5T_NATIVE_INT, scalar.id));
    const char* saved_name = "model-persist-unwritten-attribute-output.h5";
    LowFive::save(file, saved_name);
    Id saved(H5Fopen(saved_name, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id attribute(H5Aopen(saved, "unwritten", H5P_DEFAULT), H5Aclose);
    int value = -1;
    CHECK(H5Aread(attribute, H5T_NATIVE_INT, &value) >= 0);
    CHECK(value == 0);
    std::remove(saved_name);
}
void file_copy_root_token()
{
    Id fcpl(H5Pcreate(H5P_FILE_CREATE), H5Pclose);
    Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    LowFive::File source("copy-root", fcpl.id, fapl.id);
    H5R_ref_t reference{};
    source.references.emplace(
        &reference, LowFive::File::PendingReference {
            H5R_OBJECT2, source.name, source.token
        });

    LowFive::File copy(source);
    CHECK(copy.token == source.token);
    CHECK(copy.references.at(&reference).token == copy.token);
    CHECK(copy.find_token(copy.token) == &copy);
}
}
int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        if (name == "attribute-conversion") attribute_conversion();
        else if (name == "attribute-compound") attribute_compound();
        else if (name == "attribute-strings") attribute_strings();
        else if (name == "dataset-conversion") dataset_conversion();
        else if (name == "read-all") dataset_all(false);
        else if (name == "write-all") dataset_all(true);
        else if (name == "selected-strings") dataset_strings(false);
        else if (name == "zerocopy-strings") dataset_strings(true);
        else if (name == "datatype-ownership") datatype_ownership();
        else if (name == "persist-attributes") persistence(false);
        else if (name == "persist-strings") persistence(true);
        else if (name == "vlen-rewrite") vlen_rewrite();
        else if (name == "dataset-vlen-overlap") dataset_vlen_overlap();
        else if (name == "dataset-vlen-serialization") dataset_vlen_serialization();
        else if (name == "dataset-vlen-sparse-serialization") dataset_vlen_sparse_serialization();
        else if (name == "shutdown-cleanup") shutdown_cleanup();
        else if (name == "reference-top") reference_roundtrip(false);
        else if (name == "reference-compound") reference_roundtrip(true);
        else if (name == "reference-region-serialization") region_reference_serialization();
        else if (name == "hard-link-serialization") hard_link_serialization();
        else if (name == "file-copy-root-token") file_copy_root_token();
        else if (name == "reference-overwrite") pending_reference_cleanup(false);
        else if (name == "reference-delete") pending_reference_cleanup(true);
        else if (name == "dataset-reference-delete") pending_dataset_reference_cleanup();
        else if (name == "persist-references") persistence_references();
        else if (name == "persist-cross-file-reference")
            persistence_cross_file_reference();
        else if (name == "persist-overwritten-reference")
            persistence_overwritten_reference();
        else if (name == "persist-deleted-reference-path-reuse")
            persistence_deleted_reference_path_reuse();
        else if (name == "persist-selected-vlen-reference")
            persistence_selected_vlen_reference();
        else if (name == "persist-unwritten-attribute")
            persistence_unwritten_attribute();
        else CHECK(false);
    });
}
