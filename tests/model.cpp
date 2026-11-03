#include "test.hpp"
#include "vol-metadata-private.hpp"
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

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
struct UnwrittenValue
{
    int number;
    char* text;
    hvl_t sequence;
    H5R_ref_t reference;
};
Id unwritten_type(hid_t number_type)
{
    auto text = string_type();
    Id sequence(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    Id record(H5Tcreate(H5T_COMPOUND, sizeof(UnwrittenValue)), H5Tclose);
    CHECK(H5Tinsert(record, "number", HOFFSET(UnwrittenValue, number), number_type) >= 0);
    CHECK(H5Tinsert(record, "text", HOFFSET(UnwrittenValue, text), text) >= 0);
    CHECK(H5Tinsert(record, "sequence", HOFFSET(UnwrittenValue, sequence), sequence) >= 0);
    CHECK(H5Tinsert(record, "reference", HOFFSET(UnwrittenValue, reference), H5T_STD_REF) >= 0);
    hsize_t extent = 2;
    return Id(H5Tarray_create2(record, 1, &extent), H5Tclose);
}
void check_unwritten(const UnwrittenValue& value)
{
    CHECK(value.number == 0 && value.text == nullptr);
    CHECK(value.sequence.len == 0 && value.sequence.p == nullptr);
    H5R_ref_t empty{};
    CHECK(std::memcmp(&value.reference, &empty, sizeof empty) == 0);
}
void attribute_unwritten()
{
    test::Connector c("model-attribute-unwritten");
    auto file = c.file();
    auto space = test::space(2);
    Id number(H5Acreate2(file, "number", H5T_STD_I32BE, space,
                        H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    std::array<double, 3> numbers{{-1, -2, -3}};
    CHECK(H5Aread(number, H5T_NATIVE_DOUBLE, numbers.data()) >= 0);
    CHECK(numbers[0] == 0 && numbers[1] == 0 && numbers[2] == -3);

    auto text_type = string_type();
    Id text(H5Acreate2(file, "text", text_type, space,
                      H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    const char sentinel[] = "unchanged";
    std::array<char*, 3> strings{{const_cast<char*>(sentinel),
                                const_cast<char*>(sentinel),
                                const_cast<char*>(sentinel)}};
    CHECK(H5Aread(text, text_type, strings.data()) >= 0);
    CHECK(!strings[0] && !strings[1] && strings[2] == sentinel);
    CHECK(H5Treclaim(text_type, space, H5P_DEFAULT, strings.data()) >= 0);

    Id sequence_type(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    Id sequence(H5Acreate2(file, "sequence", sequence_type, space,
                          H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    int retained = 17;
    std::array<hvl_t, 3> sequences{{{1, &retained}, {1, &retained}, {1, &retained}}};
    CHECK(H5Aread(sequence, sequence_type, sequences.data()) >= 0);
    CHECK(sequences[0].len == 0 && !sequences[0].p);
    CHECK(sequences[1].len == 0 && !sequences[1].p);
    CHECK(sequences[2].len == 1 && sequences[2].p == &retained);
    CHECK(H5Treclaim(sequence_type, space, H5P_DEFAULT, sequences.data()) >= 0);

    auto disk_type = unwritten_type(H5T_STD_I32BE);
    auto memory_type = unwritten_type(H5T_NATIVE_INT);
    Id nested(H5Acreate2(file, "nested", memory_type, space,
                        H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    std::array<UnwrittenValue, 4> values;
    std::memset(values.data(), 0x5a, sizeof values);
    // Native HDF5 fills the requested representation even when no conversion
    // exists from the unwritten attribute's stored datatype.
    CHECK(H5Aread(number, memory_type, values.data()) >= 0);
    for (const auto& value : values) check_unwritten(value);
    std::memset(values.data(), 0x5a, sizeof values);
    CHECK(H5Aread(nested, disk_type, values.data()) >= 0);
    for (const auto& value : values) check_unwritten(value);
    CHECK(H5Treclaim(memory_type, space, H5P_DEFAULT, values.data()) >= 0);

    std::array<UnwrittenValue, 4> input{};
    for (auto& value : input)
    {
        value.number = 23;
        value.text = const_cast<char*>("written");
        value.sequence = {1, &retained};
    }
    CHECK(H5Awrite(nested, memory_type, input.data()) >= 0);
    CHECK(H5Awrite(nested, memory_type, input.data()) >= 0);
    CHECK(H5Aread(nested, memory_type, values.data()) >= 0);
    for (const auto& value : values)
    {
        CHECK(value.number == 23 && value.text && std::string(value.text) == "written");
        CHECK(value.sequence.len == 1 && static_cast<int*>(value.sequence.p)[0] == retained);
    }
    CHECK(H5Treclaim(memory_type, space, H5P_DEFAULT, values.data()) >= 0);

    Id empty_space(H5Screate(H5S_NULL), H5Sclose);
    Id empty(H5Acreate2(file, "empty", H5T_NATIVE_INT, empty_space,
                       H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Aread(empty, H5T_NATIVE_INT, &retained) >= 0);
    CHECK(retained == 17);
}
void attribute_unwritten_serialization()
{
    test::Connector c("model-attribute-unwritten-serialization");
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto space = test::space(2);
    auto disk_type = unwritten_type(H5T_STD_I32BE);
    auto memory_type = unwritten_type(H5T_NATIVE_INT);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT, H5P_FILE_ACCESS_DEFAULT);
    source.add_child(new LowFive::Attribute("number", H5T_STD_I32BE, space));
    source.add_child(new LowFive::Attribute("nested", memory_type, space));
    Id empty_space(H5Screate(H5S_NULL), H5Sclose);
    source.add_child(new LowFive::Attribute("empty", H5T_NATIVE_INT, empty_space));

    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, &source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* number = dynamic_cast<LowFive::Attribute*>(copy->attribute("number"));
    auto* nested = dynamic_cast<LowFive::Attribute*>(copy->attribute("nested"));
    CHECK(number && nested);
    std::array<double, 3> numbers{{-1, -2, -3}};
    number->read(LowFive::Datatype(H5T_NATIVE_DOUBLE), numbers.data());
    CHECK(numbers[0] == 0 && numbers[1] == 0 && numbers[2] == -3);
    std::array<UnwrittenValue, 4> values;
    std::memset(values.data(), 0x5a, sizeof values);
    number->read(LowFive::Datatype(memory_type), values.data());
    for (const auto& value : values) check_unwritten(value);
    std::memset(values.data(), 0x5a, sizeof values);
    nested->read(LowFive::Datatype(disk_type), values.data());
    for (const auto& value : values) check_unwritten(value);
    CHECK(H5Treclaim(memory_type, space, H5P_DEFAULT, values.data()) >= 0);
    auto* empty = dynamic_cast<LowFive::Attribute*>(copy->attribute("empty"));
    CHECK(empty);
    int untouched = 19;
    empty->read(LowFive::Datatype(H5T_NATIVE_INT), &untouched);
    CHECK(untouched == 19);

    int sequence[] = {4, 9};
    std::array<UnwrittenValue, 4> input{};
    for (auto& value : input)
    {
        value.number = 31;
        value.text = const_cast<char*>("roundtrip");
        value.sequence = {2, sequence};
    }
    nested->write(LowFive::Datatype(memory_type), input.data());
    buffer.clear();
    LowFive::serialize(buffer, copy.get(), vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> rewritten(LowFive::deserialize(buffer, vol));
    copy.reset();
    auto* written = dynamic_cast<LowFive::Attribute*>(rewritten->attribute("nested"));
    CHECK(written);
    written->read(LowFive::Datatype(memory_type), values.data());
    for (const auto& value : values)
    {
        CHECK(value.number == 31 && value.text && std::string(value.text) == "roundtrip");
        CHECK(value.sequence.len == 2 && static_cast<int*>(value.sequence.p)[1] == 9);
    }
    CHECK(H5Treclaim(memory_type, space, H5P_DEFAULT, values.data()) >= 0);
    auto* unchanged = dynamic_cast<LowFive::Attribute*>(source.attribute("nested"));
    CHECK(unchanged);
    unchanged->read(LowFive::Datatype(memory_type), values.data());
    for (const auto& value : values) check_unwritten(value);
    CHECK(H5Treclaim(memory_type, space, H5P_DEFAULT, values.data()) >= 0);
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
void dataspace_ownership()
{
    test::Connector c("model-dataspace-ownership");
    auto file = c.file();
    hsize_t extent = 1, maximum = 2;
    Id space(H5Screate_simple(1, &extent, &maximum), H5Sclose);
    Id attribute(H5Acreate2(file, "attribute", H5T_NATIVE_INT, space,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    Id dataset(H5Dcreate2(file, "dataset", H5T_NATIVE_INT, space,
                          H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int input = 17;
    CHECK(H5Awrite(attribute, H5T_NATIVE_INT, &input) >= 0);
    CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, &input) >= 0);

    extent = 2;
    CHECK(H5Sset_extent_simple(space, 1, &extent, &maximum) >= 0);
    std::array<int, 2> attribute_output{{-1, -7}};
    std::array<int, 2> dataset_output{{-1, -7}};
    CHECK(H5Aread(attribute, H5T_NATIVE_INT, attribute_output.data()) >= 0);
    CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, dataset_output.data()) >= 0);
    CHECK(attribute_output[0] == 17 && attribute_output[1] == -7);
    CHECK(dataset_output[0] == 17 && dataset_output[1] == -7);
}
void compound_background()
{
    test::Connector c("model-compound-background");
    auto file = c.file();
    auto scalar = test::scalar();
    struct Full { int a; int b; };
    Id full(H5Tcreate(H5T_COMPOUND, sizeof(Full)), H5Tclose);
    CHECK(H5Tinsert(full, "a", HOFFSET(Full, a), H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(full, "b", HOFFSET(Full, b), H5T_NATIVE_INT) >= 0);
    Id only_a(H5Tcreate(H5T_COMPOUND, sizeof(int)), H5Tclose);
    CHECK(H5Tinsert(only_a, "a", 0, H5T_NATIVE_INT) >= 0);

    Id dataset(H5Dcreate2(file, "full", full, scalar, H5P_DEFAULT,
                          H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Full initial{1, 2};
    CHECK(H5Dwrite(dataset, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &initial) >= 0);
    int replacement = 9;
    CHECK(H5Dwrite(dataset, only_a, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &replacement) >= 0);
    Full output{};
    CHECK(H5Dread(dataset, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  &output) >= 0);
    CHECK(output.a == 9 && output.b == 2);

    Id narrow(H5Dcreate2(file, "narrow", only_a, scalar, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    CHECK(H5Dwrite(narrow, only_a, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &replacement) >= 0);
    output = {-1, -7};
    CHECK(H5Dread(narrow, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  &output) >= 0);
    CHECK(output.a == 9 && output.b == -7);

    Id attribute(H5Acreate2(file, "full-attribute", full, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    CHECK(H5Awrite(attribute, full, &initial) >= 0);
    CHECK(H5Awrite(attribute, only_a, &replacement) >= 0);
    output = {};
    CHECK(H5Aread(attribute, full, &output) >= 0);
    CHECK(output.a == 9 && output.b == 2);
}
void compound_indirect_background()
{
    test::Connector c("model-compound-indirect-background");
    auto file = c.file();
    auto scalar = test::scalar();

    struct Indirect
    {
        hvl_t values;
        int retained;
    };
    Id full(H5Tcreate(H5T_COMPOUND, sizeof(Indirect)), H5Tclose);
    Id vlen(H5Tvlen_create(H5T_NATIVE_INT), H5Tclose);
    CHECK(H5Tinsert(full, "values", HOFFSET(Indirect, values), vlen) >= 0);
    CHECK(H5Tinsert(full, "retained", HOFFSET(Indirect, retained),
                    H5T_NATIVE_INT) >= 0);
    Id only_values(H5Tcreate(H5T_COMPOUND, sizeof(hvl_t)), H5Tclose);
    CHECK(H5Tinsert(only_values, "values", 0, vlen) >= 0);

    int old_values[] = {1, 2};
    int new_values[] = {8, 9, 10};
    Indirect initial{{2, old_values}, 73};
    hvl_t replacement{3, new_values};
    auto check_indirect = [&](hid_t object, bool attribute)
    {
        if (attribute)
        {
            CHECK(H5Awrite(object, full, &initial) >= 0);
            CHECK(H5Awrite(object, only_values, &replacement) >= 0);
        }
        else
        {
            CHECK(H5Dwrite(object, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           &initial) >= 0);
            CHECK(H5Dwrite(object, only_values, H5S_ALL, H5S_ALL,
                           H5P_DEFAULT, &replacement) >= 0);
        }
        Indirect output{};
        if (attribute)
            CHECK(H5Aread(object, full, &output) >= 0);
        else
            CHECK(H5Dread(object, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                          &output) >= 0);
        CHECK(output.retained == 73 && output.values.len == 3);
        CHECK(static_cast<int*>(output.values.p)[0] == 8 &&
              static_cast<int*>(output.values.p)[2] == 10);
        CHECK(H5Treclaim(full, scalar, H5P_DEFAULT, &output) >= 0);
    };

    Id attribute(H5Acreate2(file, "indirect-attribute", full, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    check_indirect(attribute, true);
    Id dataset(H5Dcreate2(file, "indirect-dataset", full, scalar,
                          H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    check_indirect(dataset, false);

    struct Referenced
    {
        int value;
        H5R_ref_t reference;
    };
    Id reference_type(H5Tcreate(H5T_COMPOUND, sizeof(Referenced)), H5Tclose);
    CHECK(H5Tinsert(reference_type, "value", HOFFSET(Referenced, value),
                    H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(reference_type, "reference",
                    HOFFSET(Referenced, reference), H5T_STD_REF) >= 0);
    Id only_value(H5Tcreate(H5T_COMPOUND, sizeof(int)), H5Tclose);
    CHECK(H5Tinsert(only_value, "value", 0, H5T_NATIVE_INT) >= 0);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Referenced referenced{};
    referenced.value = 1;
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT,
                           &referenced.reference) >= 0);
    int new_value = 9;
    auto check_reference = [&](hid_t object, bool attribute)
    {
        if (attribute)
        {
            CHECK(H5Awrite(object, reference_type, &referenced) >= 0);
            CHECK(H5Awrite(object, only_value, &new_value) >= 0);
        }
        else
        {
            CHECK(H5Dwrite(object, reference_type, H5S_ALL, H5S_ALL,
                           H5P_DEFAULT, &referenced) >= 0);
            CHECK(H5Dwrite(object, only_value, H5S_ALL, H5S_ALL,
                           H5P_DEFAULT, &new_value) >= 0);
        }
        Referenced output{};
        if (attribute)
            CHECK(H5Aread(object, reference_type, &output) >= 0);
        else
            CHECK(H5Dread(object, reference_type, H5S_ALL, H5S_ALL,
                          H5P_DEFAULT, &output) >= 0);
        CHECK(output.value == 9);
        Id opened(H5Ropen_object(&output.reference, H5P_DEFAULT,
                                 H5P_DEFAULT), H5Oclose);
        CHECK(H5Rdestroy(&output.reference) >= 0);
    };
    Id reference_attribute(
        H5Acreate2(file, "reference-attribute", reference_type, scalar,
                   H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    check_reference(reference_attribute, true);
    Id reference_dataset(
        H5Dcreate2(file, "reference-dataset", reference_type, scalar,
                   H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    check_reference(reference_dataset, false);
    CHECK(H5Rdestroy(&referenced.reference) >= 0);
}
void dataset_fill()
{
    test::Connector c("model-dataset-fill");
    auto file = c.file();
    auto space = test::space(4);
    Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    int fill = 42;
    CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &fill) >= 0);
    Id dataset(H5Dcreate2(file, "values", H5T_NATIVE_INT, space,
                          H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
    int changed_fill = 99;
    CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &changed_fill) >= 0);
    std::array<int, 4> output{{-7, -7, -7, -7}};
    CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, output.data()) >= 0);
    CHECK((output == std::array<int, 4>{{42, 42, 42, 42}}));

    auto scalar = test::scalar();
    Id converted_dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    double fractional_fill = 1.75;
    CHECK(H5Pset_fill_value(converted_dcpl, H5T_NATIVE_DOUBLE,
                            &fractional_fill) >= 0);
    Id converted(H5Dcreate2(file, "converted", H5T_NATIVE_INT, scalar,
                            H5P_DEFAULT, converted_dcpl, H5P_DEFAULT),
                 H5Dclose);
    double converted_output = -1;
    CHECK(H5Dread(converted, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, &converted_output) >= 0);
    CHECK(converted_output == 1.0);

    struct Full
    {
        int a;
        int b;
    };
    Id narrow(H5Tcreate(H5T_COMPOUND, sizeof(int)), H5Tclose);
    CHECK(H5Tinsert(narrow, "a", 0, H5T_NATIVE_INT) >= 0);
    Id full(H5Tcreate(H5T_COMPOUND, sizeof(Full)), H5Tclose);
    CHECK(H5Tinsert(full, "a", HOFFSET(Full, a), H5T_NATIVE_INT) >= 0);
    CHECK(H5Tinsert(full, "b", HOFFSET(Full, b), H5T_NATIVE_INT) >= 0);
    Id compound_dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    int compound_fill = 5;
    CHECK(H5Pset_fill_value(compound_dcpl, narrow, &compound_fill) >= 0);
    Id compound(H5Dcreate2(file, "compound", narrow, scalar, H5P_DEFAULT,
                           compound_dcpl, H5P_DEFAULT), H5Dclose);
    Full compound_output{-1, -7};
    CHECK(H5Dread(compound, full, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  &compound_output) >= 0);
    CHECK(compound_output.a == 5 && compound_output.b == -7);
}
void dataset_shrink()
{
    test::Connector c("model-dataset-shrink");
    auto file = c.file();
    hsize_t extent = 4, maximum = 4, chunk = 2;
    Id space(H5Screate_simple(1, &extent, &maximum), H5Sclose);
    Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    int fill = 42;
    CHECK(H5Pset_chunk(dcpl, 1, &chunk) >= 0);
    CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &fill) >= 0);
    Id dataset(H5Dcreate2(file, "values", H5T_NATIVE_INT, space,
                          H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
    std::array<int, 4> input{{1, 2, 3, 4}};
    CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, input.data()) >= 0);
    extent = 2;
    CHECK(H5Dset_extent(dataset, &extent) >= 0);
    extent = 4;
    CHECK(H5Dset_extent(dataset, &extent) >= 0);
    std::array<int, 4> output{};
    CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, output.data()) >= 0);
    CHECK((output == std::array<int, 4>{{1, 2, 42, 42}}));
}
void scalar_selections()
{
    test::Connector c("model-scalar-selections");
    auto file = c.file();
    auto scalar = test::scalar();
    Id dataset(H5Dcreate2(file, "value", H5T_NATIVE_INT, scalar,
                          H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    const int expected = 42;
    CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, scalar, scalar,
                   H5P_DEFAULT, &expected) >= 0);
    Id file_space(H5Dget_space(dataset), H5Sclose);
    int actual = -1;
    CHECK(H5Dread(dataset, H5T_NATIVE_INT, scalar, file_space,
                  H5P_DEFAULT, &actual) >= 0);
    CHECK(actual == expected);

    CHECK(H5Sselect_none(scalar) >= 0);
    CHECK(H5Sselect_none(file_space) >= 0);
    actual = -7;
    CHECK(H5Dread(dataset, H5T_NATIVE_INT, scalar, file_space,
                  H5P_DEFAULT, &actual) >= 0);
    CHECK(actual == -7);

    Id null_space(H5Screate(H5S_NULL), H5Sclose);
    Id empty(H5Dcreate2(file, "empty", H5T_NATIVE_INT, null_space,
                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    CHECK(H5Dread(empty, H5T_NATIVE_INT, null_space, null_space,
                  H5P_DEFAULT, &actual) >= 0);
    CHECK(actual == -7);
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
    const char* replacement[] = {"replacement", nullptr};
    CHECK(H5Dwrite(d, type, ms, fs, H5P_DEFAULT, replacement) >= 0);
    CHECK(H5Dread(d, type, one, fs, H5P_DEFAULT, &out) >= 0);
    CHECK(out && std::string(out) == "replacement");
    CHECK(H5Treclaim(type, one, H5P_DEFAULT, &out) >= 0);
}
void datatype_ownership()
{
    hid_t raw = H5Tcopy(H5T_NATIVE_INT); CHECK(raw >= 0);
    { LowFive::Datatype owned(raw, true); CHECK(H5Iis_valid(raw) > 0); }
    CHECK(H5Iis_valid(raw) == 0);
}
void hid_moves()
{
    static_assert(std::is_nothrow_move_constructible<LowFive::Hid>::value);
    static_assert(std::is_nothrow_move_assignable<LowFive::Hid>::value);
    const hid_t raw = H5Pcreate(H5P_DATASET_CREATE);
    const hid_t old_target = H5Pcreate(H5P_DATASET_ACCESS);
    CHECK(raw >= 0 && old_target >= 0);
    {
        LowFive::Hid source(raw, true);
        CHECK(H5Iget_ref(raw) == 1);
        LowFive::Hid moved(std::move(source));
        CHECK(source.id == 0 && moved.id == raw && H5Iget_ref(raw) == 1);
        LowFive::Hid target(old_target, true);
        target = std::move(moved);
        CHECK(moved.id == 0 && target.id == raw && H5Iget_ref(raw) == 1);
        CHECK(H5Iis_valid(old_target) == 0);
        auto* self = &target;
        target = std::move(*self);
        CHECK(target.id == raw && H5Iget_ref(raw) == 1);
        LowFive::Hid shared(target);
        CHECK(H5Iget_ref(raw) == 2);
        target = std::move(shared);
        CHECK(shared.id == 0 && target.id == raw && H5Iget_ref(raw) == 1);
        LowFive::Hid empty(0);
        target = std::move(empty);
        CHECK(target.id == 0 && empty.id == 0 && H5Iis_valid(raw) == 0);
        LowFive::Hid moved_empty(std::move(target));
        CHECK(target.id == 0 && moved_empty.id == 0);
    }
    const hid_t final = H5Screate(H5S_SCALAR);
    CHECK(final >= 0);
    {
        LowFive::Hid source(final, true);
        LowFive::Hid destination(std::move(source));
        CHECK(source.id == 0 && destination.id == final && H5Iget_ref(final) == 1);
    }
    CHECK(H5Iis_valid(final) == 0);
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
void dataset_fill_reference_serialization()
{
    test::Connector c("model-dataset-fill-reference-serialization");
    auto file = c.file();
    auto scalar = test::scalar();
    auto extent = test::space(2);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, scalar,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    int input = 73;
    CHECK(H5Dwrite(target, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, &input) >= 0);
    H5R_ref_t reference{};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &reference) >= 0);
    Id properties(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    CHECK(H5Pset_fill_value(properties, H5T_STD_REF, &reference) >= 0);
    Id dataset(H5Dcreate2(file, "references", H5T_STD_REF, extent,
                          H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
    std::array<H5R_ref_t, 2> initial{};
    CHECK(H5Dread(dataset, H5T_STD_REF, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, initial.data()) >= 0);
    for (auto& value : initial)
    {
        CHECK(H5Rget_type(&value) == H5R_OBJECT2);
        CHECK(H5Rdestroy(&value) >= 0);
    }
    CHECK(H5Rget_type(&reference) == H5R_OBJECT2);

    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(c.filename));
    CHECK(source);
    source->copy_whole = true;
    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, source, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> first(LowFive::deserialize(buffer, vol));
    buffer.clear();
    LowFive::serialize(buffer, first.get(), vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    first.reset();
    auto* tree = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(tree && tree->references.size() == 1);

    CHECK(H5Rdestroy(&reference) >= 0);
    properties.reset();
    dataset.reset();
    target.reset();
    file.reset();
    vol.clear_files();
    CHECK(vol.files.emplace(c.filename, copy.release()).second);

    Id reopened(H5Fopen(c.filename.c_str(), H5F_ACC_RDONLY, c.fapl), H5Fclose);
    Id references(H5Dopen2(reopened, "references", H5P_DEFAULT), H5Dclose);
    auto check_reference = [&](H5R_ref_t& value)
    {
        CHECK(H5Rget_type(&value) == H5R_OBJECT2);
        Id opened(H5Ropen_object(&value, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        int output = -1;
        CHECK(H5Dread(opened, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, &output) >= 0);
        CHECK(output == input);
    };
    std::array<H5R_ref_t, 2> output{};
    CHECK(H5Dread(references, H5T_STD_REF, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, output.data()) >= 0);
    for (auto& value : output)
    {
        check_reference(value);
        CHECK(H5Rdestroy(&value) >= 0);
    }
    CHECK(tree->references.size() == 1);
    CHECK(!tree->references.begin()->second.resolved);

    Id copied_properties(H5Dget_create_plist(references), H5Pclose);
    H5D_fill_value_t status;
    CHECK(H5Pfill_value_defined(copied_properties, &status) >= 0);
    CHECK(status == H5D_FILL_VALUE_USER_DEFINED);
    H5R_ref_t borrowed{};
    CHECK(H5Pget_fill_value(copied_properties, H5T_STD_REF, &borrowed) >= 0);
    CHECK(H5Rcopy(&borrowed, &reference) >= 0);
    check_reference(reference);
    CHECK(H5Rdestroy(&reference) >= 0);
    Id retained_properties(H5Pcopy(copied_properties), H5Pclose);
    copied_properties.reset();
    references.reset();
    reopened.reset();
    CHECK(H5Pget_fill_value(retained_properties, H5T_STD_REF, &borrowed) >= 0);
    CHECK(H5Rcopy(&borrowed, &reference) >= 0);
    check_reference(reference);
    CHECK(H5Rdestroy(&reference) >= 0);
    CHECK(!tree->references.begin()->second.resolved);
}
void dataset_properties_stale_reference()
{
    test::Connector c("model-dataset-properties-stale-reference");
    auto file = c.file();
    auto scalar = test::scalar();
    Id first(H5Dcreate2(file, "first", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                        H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id second(H5Dcreate2(file, "second", H5T_NATIVE_INT, scalar, H5P_DEFAULT,
                         H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id dataset(H5Dcreate2(file, "references", H5T_STD_REF, scalar,
                          H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    H5R_ref_t reference{};
    CHECK(H5Rcreate_object(file, "first", H5P_DEFAULT, &reference) >= 0);
    CHECK(H5Dwrite(dataset, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &reference) >= 0);
    CHECK(H5Rdestroy(&reference) >= 0);
    CHECK(H5Rcreate_object(file, "second", H5P_DEFAULT, &reference) >= 0);
    CHECK(H5Dwrite(dataset, H5T_STD_REF, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                   &reference) >= 0);
    CHECK(H5Rdestroy(&reference) >= 0);
    first.reset();
    CHECK(H5Ldelete(file, "first", H5P_DEFAULT) >= 0);
    Id properties(H5Dget_create_plist(dataset), H5Pclose);
    H5D_fill_value_t status;
    CHECK(H5Pfill_value_defined(properties, &status) >= 0);
    CHECK(status == H5D_FILL_VALUE_DEFAULT);
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
    test::Connector c("model-persist-unwritten-attribute");
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(c.vol);
    auto scalar = test::scalar();
    auto type = unwritten_type(H5T_NATIVE_INT);
    LowFive::File file(
        "model-persist-unwritten-attribute", H5P_FILE_CREATE_DEFAULT,
        H5P_FILE_ACCESS_DEFAULT);
    file.add_child(new LowFive::Attribute(
        "unwritten", H5T_STD_I32BE, scalar.id));
    file.add_child(new LowFive::Attribute("nested", type, scalar));
    auto* original = dynamic_cast<LowFive::Attribute*>(file.attribute("nested"));
    CHECK(original);
    std::array<UnwrittenValue, 2> output;
    std::memset(output.data(), 0x5a, sizeof output);
    original->read(LowFive::Datatype(type), output.data());
    for (const auto& value : output) check_unwritten(value);

    diy::MemoryBuffer buffer;
    LowFive::serialize(buffer, &file, vol);
    buffer.reset();
    std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
    auto* copied_file = dynamic_cast<LowFive::File*>(copy.get());
    CHECK(copied_file);
    const char* saved_name = "model-persist-unwritten-attribute-output.h5";
    for (const auto* source : {&file, copied_file})
    {
        LowFive::save(*source, saved_name);
        Id saved(H5Fopen(saved_name, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
        Id attribute(H5Aopen(saved, "unwritten", H5P_DEFAULT), H5Aclose);
        double number = -1;
        CHECK(H5Aread(attribute, H5T_NATIVE_DOUBLE, &number) >= 0);
        CHECK(number == 0);
        Id nested(H5Aopen(saved, "nested", H5P_DEFAULT), H5Aclose);
        std::memset(output.data(), 0x5a, sizeof output);
        CHECK(H5Aread(nested, type, output.data()) >= 0);
        for (const auto& value : output) check_unwritten(value);
        CHECK(H5Treclaim(type, scalar, H5P_DEFAULT, output.data()) >= 0);
    }
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
void persistence_properties()
{
    auto scalar = test::scalar();
    hsize_t extent = 4, maximum = H5S_UNLIMITED, chunk = 2;
    Id space(H5Screate_simple(1, &extent, &maximum), H5Sclose);
    Id dcpl(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    int fill = 42;
    CHECK(H5Pset_chunk(dcpl, 1, &chunk) >= 0);
    CHECK(H5Pset_fletcher32(dcpl) >= 0);
    CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &fill) >= 0);
    CHECK(H5Pset_fill_time(dcpl, H5D_FILL_TIME_ALLOC) >= 0);
    CHECK(H5Pset_alloc_time(dcpl, H5D_ALLOC_TIME_INCR) >= 0);
    CHECK(H5Pset_attr_creation_order(
        dcpl, H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED) >= 0);
    Id gcpl(H5Pcreate(H5P_GROUP_CREATE), H5Pclose);
    CHECK(H5Pset_link_creation_order(
        gcpl, H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED) >= 0);
    CHECK(H5Pset_attr_creation_order(
        gcpl, H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED) >= 0);
    CHECK(H5Pset_link_phase_change(gcpl, 6, 3) >= 0);
    CHECK(H5Pset_attr_phase_change(gcpl, 5, 2) >= 0);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT,
                         H5P_FILE_ACCESS_DEFAULT);
    auto* group = new LowFive::Group("group", gcpl);
    source.add_child(group);
    auto* dataset = new LowFive::Dataset(
        "values", H5T_NATIVE_INT, space, LowFive::Dataset::lowfive,
        LowFive::Hid(dcpl), LowFive::Hid(H5P_DEFAULT), false, true);
    group->add_child(dataset);
    group->add_child(new LowFive::Group("second", H5P_DEFAULT));
    for (const char* name : {"zeta", "alpha"})
    {
        auto* attribute = new LowFive::Attribute(name, H5T_NATIVE_INT, scalar);
        dataset->add_child(attribute);
        attribute->write(LowFive::Datatype(H5T_NATIVE_INT), &fill);
    }
    int changed = 99;
    CHECK(H5Pset_fill_value(dcpl, H5T_NATIVE_INT, &changed) >= 0);
    const char* filename = "model-persist-properties.h5";
    LowFive::save(source, filename);
    Id file(H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id saved(H5Dopen2(file, "group/values", H5P_DEFAULT), H5Dclose);
    std::array<int, 4> values{};
    CHECK(H5Dread(saved, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, values.data()) >= 0);
    for (int value : values) CHECK(value == 42);
    Id properties(H5Dget_create_plist(saved), H5Pclose);
    CHECK(H5Pget_layout(properties) == H5D_CHUNKED);
    hsize_t actual_chunk = 0;
    CHECK(H5Pget_chunk(properties, 1, &actual_chunk) == 1);
    CHECK(actual_chunk == chunk);
    CHECK(H5Pget_nfilters(properties) == 1);
    unsigned flags = 0, config = 0;
    size_t parameters = 0;
    CHECK(H5Pget_filter2(properties, 0, &flags, &parameters, nullptr, 0,
                          nullptr, &config) == H5Z_FILTER_FLETCHER32);
    CHECK(H5Pget_fill_value(properties, H5T_NATIVE_INT, &changed) >= 0);
    CHECK(changed == 42);
    H5D_fill_time_t fill_time;
    H5D_alloc_time_t alloc_time;
    CHECK(H5Pget_fill_time(properties, &fill_time) >= 0);
    CHECK(fill_time == H5D_FILL_TIME_ALLOC);
    CHECK(H5Pget_alloc_time(properties, &alloc_time) >= 0);
    CHECK(alloc_time == H5D_ALLOC_TIME_INCR);
    Id saved_space(H5Dget_space(saved), H5Sclose);
    hsize_t actual_extent = 0, actual_maximum = 0;
    CHECK(H5Sget_simple_extent_dims(saved_space, &actual_extent,
                                    &actual_maximum) == 1);
    CHECK(actual_extent == extent && actual_maximum == maximum);
    Id saved_group(H5Gopen2(file, "group", H5P_DEFAULT), H5Gclose);
    Id group_properties(H5Gget_create_plist(saved_group), H5Pclose);
    unsigned order = 0, high = 0, low = 0;
    CHECK(H5Pget_link_creation_order(group_properties, &order) >= 0);
    CHECK(order == (H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED));
    CHECK(H5Pget_link_phase_change(group_properties, &high, &low) >= 0);
    CHECK(high == 6 && low == 3);
    CHECK(H5Pget_attr_creation_order(group_properties, &order) >= 0);
    CHECK(order == (H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED));
    CHECK(H5Pget_attr_phase_change(group_properties, &high, &low) >= 0);
    CHECK(high == 5 && low == 2);
    for (unsigned i = 0; i < 2; ++i)
    {
        Id attribute(H5Aopen_by_idx(saved, ".", H5_INDEX_CRT_ORDER,
                                    H5_ITER_INC, i, H5P_DEFAULT,
                                    H5P_DEFAULT), H5Aclose);
        char name[16]{};
        CHECK(H5Aget_name(attribute, sizeof name, name) >= 0);
        CHECK(std::string(name) == (i ? "alpha" : "zeta"));
    }
    std::remove(filename);
}

void check_same_object(hid_t file, const char* first, const char* second)
{
    H5O_info2_t a{}, b{};
    CHECK(H5Oget_info_by_name3(file, first, &a, H5O_INFO_BASIC,
                               H5P_DEFAULT) >= 0);
    CHECK(H5Oget_info_by_name3(file, second, &b, H5O_INFO_BASIC,
                               H5P_DEFAULT) >= 0);
    int comparison = 1;
    CHECK(H5Otoken_cmp(file, &a.token, &b.token, &comparison) >= 0);
    CHECK(comparison == 0);
}

void persistence_links()
{
    auto scalar = test::scalar();
    Id gcpl(H5Pcreate(H5P_GROUP_CREATE), H5Pclose);
    CHECK(H5Pset_link_creation_order(
        gcpl, H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED) >= 0);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT,
                         H5P_FILE_ACCESS_DEFAULT);
    auto* group = new LowFive::Group("group", gcpl);
    source.add_child(group);
    auto* dataset = new LowFive::Dataset(
        "target", H5T_NATIVE_INT, scalar, LowFive::Dataset::lowfive,
        LowFive::Hid(H5P_DEFAULT), LowFive::Hid(H5P_DEFAULT), false, true);
    group->add_child(new LowFive::HardLink("forward", dataset));
    group->add_child(new LowFive::SoftLink("relative", "target"));
    group->add_child(dataset);
    group->add_child(new LowFive::HardLink("self", group));
    group->add_child(new LowFive::SoftLink("dangling", "../missing"));
    group->add_child(new LowFive::SoftLink("soft-cycle", "soft-cycle"));
    auto* detached = new LowFive::Group("old-name", H5P_DEFAULT);
    source.unlinked.push_back(detached);
    detached->add_child(new LowFive::Group("child", H5P_DEFAULT));
    source.add_child(new LowFive::HardLink("detached", detached));
    int input = 19;
    dataset->write(LowFive::Datatype(H5T_NATIVE_INT),
                   LowFive::Dataspace(scalar), LowFive::Dataspace(scalar),
                   &input);
    const char* filename = "model-persist-links.h5";
    LowFive::save(source, filename);
    Id file(H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    check_same_object(file, "/group/forward", "/group/target");
    check_same_object(file, "/group", "/group/self");
    check_same_object(file, "/group/relative", "/group/target");
    CHECK(H5Lexists(file, "/detached/child", H5P_DEFAULT) > 0);
    CHECK(H5Lexists(file, "/old-name", H5P_DEFAULT) == 0);
    for (const auto& link : std::array<std::pair<const char*, const char*>, 3>{
             {{"relative", "target"}, {"dangling", "../missing"},
              {"soft-cycle", "soft-cycle"}}})
    {
        const std::string path = std::string("/group/") + link.first;
        H5L_info2_t info{};
        CHECK(H5Lget_info2(file, path.c_str(), &info, H5P_DEFAULT) >= 0);
        CHECK(info.type == H5L_TYPE_SOFT);
        std::vector<char> target(info.u.val_size);
        CHECK(H5Lget_val(file, path.c_str(), target.data(), target.size(),
                          H5P_DEFAULT) >= 0);
        CHECK(std::string(target.data()) == link.second);
    }
    const std::array<const char*, 6> order{
        {"forward", "relative", "target", "self", "dangling", "soft-cycle"}};
    for (size_t i = 0; i < order.size(); ++i)
    {
        char name[32]{};
        CHECK(H5Lget_name_by_idx(file, "/group", H5_INDEX_CRT_ORDER,
                                  H5_ITER_INC, i, name, sizeof name,
                                  H5P_DEFAULT) >= 0);
        CHECK(std::string(name) == order[i]);
    }
    Id saved(H5Dopen2(file, "/group/forward", H5P_DEFAULT), H5Dclose);
    int output = 0;
    CHECK(H5Dread(saved, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, &output) >= 0);
    CHECK(output == input);
    std::remove(filename);
}

void persistence_committed_types()
{
    auto scalar = test::scalar();
    Id type(H5Tcreate(H5T_COMPOUND, sizeof(int)), H5Tclose);
    CHECK(H5Tinsert(type, "number", 0, H5T_NATIVE_INT) >= 0);
    LowFive::File source("source", H5P_FILE_CREATE_DEFAULT,
                         H5P_FILE_ACCESS_DEFAULT);
    auto* committed = new LowFive::CommittedDatatype("record", type);
    source.add_child(new LowFive::HardLink("alias", committed));
    source.add_child(committed);
    auto* attribute = new LowFive::Attribute("version", H5T_NATIVE_INT, scalar);
    committed->add_child(attribute);
    int version = 7;
    attribute->write(LowFive::Datatype(H5T_NATIVE_INT), &version);
    auto* reference_attribute =
        new LowFive::Attribute("type-reference", H5T_STD_REF, scalar);
    source.add_child(reference_attribute);
    H5R_ref_t empty{};
    reference_attribute->write(LowFive::Datatype(H5T_STD_REF), &empty);
    LowFive::File::PendingReference state;
    state.type = H5R_OBJECT2;
    state.token = committed->token;
    source.references.emplace(
        reinterpret_cast<H5R_ref_t*>(reference_attribute->data.get()), state);
    const char* filename = "model-persist-committed-types.h5";
    LowFive::save(source, filename);
    Id file(H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    Id saved(H5Topen2(file, "record", H5P_DEFAULT), H5Tclose);
    CHECK(H5Tcommitted(saved) > 0);
    CHECK(H5Tequal(saved, type) > 0);
    check_same_object(file, "record", "alias");
    Id saved_attribute(H5Aopen(saved, "version", H5P_DEFAULT), H5Aclose);
    int output = 0;
    CHECK(H5Aread(saved_attribute, H5T_NATIVE_INT, &output) >= 0);
    CHECK(output == version);
    Id saved_reference(H5Aopen(file, "type-reference", H5P_DEFAULT), H5Aclose);
    H5R_ref_t reference{};
    CHECK(H5Aread(saved_reference, H5T_STD_REF, &reference) >= 0);
    Id referenced(H5Ropen_object(&reference, H5P_DEFAULT, H5P_DEFAULT), H5Tclose);
    CHECK(H5Tcommitted(referenced) > 0);
    CHECK(H5Tequal(referenced, saved) > 0);
    CHECK(H5Rdestroy(&reference) >= 0);
    std::remove(filename);
}

void persistence_fill_references()
{
    test::Connector connector("model-persist-fill-references");
    auto file = connector.file();
    auto scalar = test::scalar();
    auto extent = test::space(3);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, extent,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    std::array<int, 3> input{{17, 23, 31}};
    CHECK(H5Dwrite(target, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                   H5P_DEFAULT, input.data()) >= 0);
    Id attribute(H5Acreate2(target, "version", H5T_NATIVE_INT, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    int version = 8;
    CHECK(H5Awrite(attribute, H5T_NATIVE_INT, &version) >= 0);
    std::array<H5R_ref_t, 3> fill{};
    CHECK(H5Rcreate_object(file, "target", H5P_DEFAULT, &fill[0]) >= 0);
    select(extent, 1, 2);
    CHECK(H5Rcreate_region(file, "target", extent, H5P_DEFAULT, &fill[1]) >= 0);
    CHECK(H5Rcreate_attr(file, "target", "version", H5P_DEFAULT, &fill[2]) >= 0);
    const std::array<const char*, 3> names{{
        "object-reference", "region-reference", "attribute-reference"}};
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(connector.filename));
    CHECK(source);
    source->copy_whole = true;
    const char* filename = "model-persist-fill-references.h5";
    auto rejected = [&](const LowFive::File& metadata)
    {
        bool failed = false;
        try { LowFive::save(metadata, filename); }
        catch (const LowFive::MetadataError&) { failed = true; }
        CHECK(failed);
    };
    for (size_t i = 0; i < fill.size(); ++i)
    {
        Id properties(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
        CHECK(H5Pset_fill_value(properties, H5T_STD_REF, &fill[i]) >= 0);
        Id references(H5Dcreate2(file, names[i], H5T_STD_REF, scalar,
                                 H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
        rejected(*source);
        rejected(*source);
        diy::MemoryBuffer buffer;
        LowFive::serialize(buffer, source, vol);
        buffer.reset();
        std::unique_ptr<LowFive::Object> copy(LowFive::deserialize(buffer, vol));
        auto* restored = dynamic_cast<LowFive::File*>(copy.get());
        CHECK(restored && restored->references.size() == 1);
        rejected(*restored);
        rejected(*restored);
        CHECK(restored->references.size() == 1);
        H5R_ref_t borrowed{};
        CHECK(H5Pget_fill_value(properties, H5T_STD_REF, &borrowed) >= 0);
        CHECK(H5Rget_type(&borrowed) == H5Rget_type(&fill[i]));
        references.reset();
        CHECK(H5Ldelete(file, names[i], H5P_DEFAULT) >= 0);
    }
    // Neither live nor pending rejection may consume source references.
    Id opened(H5Ropen_object(&fill[0], H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    std::array<int, 3> values{};
    CHECK(H5Dread(opened, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                  H5P_DEFAULT, values.data()) >= 0);
    CHECK(values == input);
    Id region(H5Ropen_region(&fill[1], H5P_DEFAULT, H5P_DEFAULT), H5Sclose);
    hsize_t start = 0, end = 0;
    CHECK(H5Sget_select_bounds(region, &start, &end) >= 0);
    CHECK(start == 1 && end == 2);
    Id opened_attribute(H5Ropen_attr(&fill[2], H5P_DEFAULT,
                                     H5P_DEFAULT), H5Aclose);
    int actual = 0;
    CHECK(H5Aread(opened_attribute, H5T_NATIVE_INT, &actual) >= 0);
    CHECK(actual == version);
    for (auto& reference : fill) CHECK(H5Rdestroy(&reference) >= 0);
    std::remove(filename);
}

void persistence_nested_fill_references()
{
    test::Connector connector("model-persist-nested-fill-references");
    auto file = connector.file();
    auto scalar = test::scalar();
    auto extent = test::space(3);
    Id target(H5Dcreate2(file, "target", H5T_NATIVE_INT, extent,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    Id attribute(H5Acreate2(target, "version", H5T_NATIVE_INT, scalar,
                            H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    int version = 8;
    CHECK(H5Awrite(attribute, H5T_NATIVE_INT, &version) >= 0);
    std::array<H5R_ref_t, 2> fill{};
    select(extent, 1, 2);
    CHECK(H5Rcreate_region(file, "target", extent, H5P_DEFAULT, &fill[0]) >= 0);
    CHECK(H5Rcreate_attr(file, "target", "version", H5P_DEFAULT, &fill[1]) >= 0);
    hsize_t count = fill.size();
    Id array(H5Tarray_create2(H5T_STD_REF, 1, &count), H5Tclose);
    Id compound(H5Tcreate(H5T_COMPOUND, sizeof(fill)), H5Tclose);
    CHECK(H5Tinsert(compound, "region", 0, H5T_STD_REF) >= 0);
    CHECK(H5Tinsert(compound, "attribute", sizeof(H5R_ref_t), H5T_STD_REF) >= 0);
    auto& vol = dynamic_cast<LowFive::MetadataVOL&>(connector.vol);
    auto* source = dynamic_cast<LowFive::File*>(vol.files.at(connector.filename));
    CHECK(source);
    for (hid_t type : {static_cast<hid_t>(array), static_cast<hid_t>(compound)})
    {
        Id properties(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
        CHECK(H5Pset_fill_value(properties, type, fill.data()) >= 0);
        Id references(H5Dcreate2(file, "references", type, scalar,
                                 H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
        bool failed = false;
        try { LowFive::save(*source, "model-persist-nested-fill-references.h5"); }
        catch (const LowFive::MetadataError&) { failed = true; }
        CHECK(failed);
        // Rejection must not consume either the original or DCPL references.
        std::array<H5R_ref_t, 2> borrowed{};
        CHECK(H5Pget_fill_value(properties, type, borrowed.data()) >= 0);
        CHECK(H5Rget_type(&borrowed[0]) == H5R_DATASET_REGION2);
        CHECK(H5Rget_type(&borrowed[1]) == H5R_ATTR);
        Id region(H5Ropen_region(&fill[0], H5P_DEFAULT, H5P_DEFAULT), H5Sclose);
        hsize_t start = 0, end = 0;
        CHECK(H5Sget_select_bounds(region, &start, &end) >= 0);
        CHECK(start == 1 && end == 2);
        Id opened(H5Ropen_attr(&fill[1], H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        int actual = 0;
        CHECK(H5Aread(opened, H5T_NATIVE_INT, &actual) >= 0);
        CHECK(actual == version);
        references.reset();
        CHECK(H5Ldelete(file, "references", H5P_DEFAULT) >= 0);
    }
    for (auto& reference : fill) CHECK(H5Rdestroy(&reference) >= 0);
    std::remove("model-persist-nested-fill-references.h5");
}

void persistence_unsupported_object()
{
    auto scalar = test::scalar();
    auto rejected = [&](LowFive::Object* object)
    {
        LowFive::File source("source", H5P_FILE_CREATE_DEFAULT,
                             H5P_FILE_ACCESS_DEFAULT);
        source.add_child(object);
        bool failed = false;
        try { LowFive::save(source, "model-persist-unsupported-object.h5"); }
        catch (const LowFive::MetadataError&) { failed = true; }
        CHECK(failed);
    };
    rejected(new LowFive::NamedDtype("missing-definition"));
    rejected(new LowFive::CommittedDatatype("empty-definition", 0));
    rejected(new LowFive::HardLink("missing-target", nullptr));
    rejected(new LowFive::Dataset(
        "legacy-reference", H5T_STD_REF_OBJ, scalar, LowFive::Dataset::lowfive,
        LowFive::Hid(H5P_DEFAULT), LowFive::Hid(H5P_DEFAULT), false, true));
    std::remove("model-persist-unsupported-object.h5");
}
}
int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        if (name == "attribute-conversion") attribute_conversion();
        else if (name == "attribute-compound") attribute_compound();
        else if (name == "attribute-strings") attribute_strings();
        else if (name == "attribute-unwritten") attribute_unwritten();
        else if (name == "attribute-unwritten-serialization") attribute_unwritten_serialization();
        else if (name == "dataset-conversion") dataset_conversion();
        else if (name == "dataspace-ownership") dataspace_ownership();
        else if (name == "compound-background") compound_background();
        else if (name == "compound-indirect-background")
            compound_indirect_background();
        else if (name == "dataset-fill") dataset_fill();
        else if (name == "dataset-shrink") dataset_shrink();
        else if (name == "scalar-selections") scalar_selections();
        else if (name == "read-all") dataset_all(false);
        else if (name == "write-all") dataset_all(true);
        else if (name == "selected-strings") dataset_strings(false);
        else if (name == "zerocopy-strings") dataset_strings(true);
        else if (name == "datatype-ownership") datatype_ownership();
        else if (name == "hid-moves") hid_moves();
        else if (name == "persist-attributes") persistence(false);
        else if (name == "persist-strings") persistence(true);
        else if (name == "vlen-rewrite") vlen_rewrite();
        else if (name == "dataset-vlen-overlap") dataset_vlen_overlap();
        else if (name == "dataset-vlen-serialization") dataset_vlen_serialization();
        else if (name == "dataset-vlen-sparse-serialization") dataset_vlen_sparse_serialization();
        else if (name == "shutdown-cleanup") shutdown_cleanup();
        else if (name == "reference-top") reference_roundtrip(false);
        else if (name == "reference-compound") reference_roundtrip(true);
        else if (name == "dataset-fill-reference-serialization")
            dataset_fill_reference_serialization();
        else if (name == "dataset-properties-stale-reference")
            dataset_properties_stale_reference();
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
        else if (name == "persist-properties") persistence_properties();
        else if (name == "persist-links") persistence_links();
        else if (name == "persist-committed-types") persistence_committed_types();
        else if (name == "persist-fill-references") persistence_fill_references();
        else if (name == "persist-nested-fill-references")
            persistence_nested_fill_references();
        else if (name == "persist-unsupported-object") persistence_unsupported_object();
        else CHECK(false);
    });
}
