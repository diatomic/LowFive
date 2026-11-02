#include "test.hpp"
#include <array>
#include <vector>

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& mode)
    {
        if (mode == "patterns")
        {
            CHECK(LowFive::MetadataVOL::match("", ""));
            CHECK(!LowFive::MetadataVOL::match("", "a"));
            CHECK(LowFive::MetadataVOL::match("***", ""));
            CHECK(!LowFive::MetadataVOL::match("?", ""));
            CHECK(LowFive::MetadataVOL::match("?", "*"));
            CHECK(!LowFive::MetadataVOL::match("a?x", "a"));
            CHECK(LowFive::MetadataVOL::match("data/*.h?", "data/run.h5"));
            CHECK(!LowFive::MetadataVOL::match("data/*.h?", "data/run.h55"));
            CHECK(LowFive::MetadataVOL::match("*ab*ac", "ababac"));
            CHECK(LowFive::MetadataVOL::match("**a**?*", "*ab"));
            const std::string long_path(100000, 'a');
            CHECK(LowFive::MetadataVOL::match("*a", long_path.c_str()));
            CHECK(!LowFive::MetadataVOL::match("*a*a*a*a*a*a*a*a*a*a*b",
                                               long_path.c_str()));
            return;
        }
        test::Connector connector("api-" + mode, mode);
        auto file = connector.file();
        test::Id group(H5Gcreate2(file, "group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
        auto space = test::space(6);
        test::Id dataset(H5Dcreate2(group, "values", H5T_NATIVE_INT, space,
                                   H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        const std::array<int, 6> input{{1, 2, 3, 4, 5, 6}};
        CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, input.data()) >= 0);
        std::array<int, 6> output{};
        CHECK(H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, output.data()) >= 0);
        CHECK(output == input);

        test::Id file_space(H5Dget_space(dataset), H5Sclose);
        hsize_t start = 2, count = 2;
        CHECK(H5Sselect_hyperslab(file_space, H5S_SELECT_SET, &start, nullptr, &count, nullptr) >= 0);
        auto memory_space = test::space(2);
        std::array<int, 2> selected{};
        CHECK(H5Dread(dataset, H5T_NATIVE_INT, memory_space, file_space, H5P_DEFAULT, selected.data()) >= 0);
        CHECK((selected == std::array<int, 2>{{3, 4}}));
        const std::array<int, 2> replacement{{30, 40}};
        CHECK(H5Dwrite(dataset, H5T_NATIVE_INT, memory_space, file_space, H5P_DEFAULT, replacement.data()) >= 0);
        CHECK(H5Dread(dataset, H5T_NATIVE_INT, memory_space, file_space, H5P_DEFAULT, selected.data()) >= 0);
        CHECK(selected == replacement);

        auto attr_space = test::space(2);
        test::Id attr(H5Acreate2(dataset, "units", H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
        const std::array<int, 2> units{{7, 9}};
        CHECK(H5Awrite(attr, H5T_NATIVE_INT, units.data()) >= 0);
        std::array<int, 2> units_out{};
        CHECK(H5Aread(attr, H5T_NATIVE_INT, units_out.data()) >= 0);
        CHECK(units_out == units);
        CHECK(H5Aexists(dataset, "units") > 0);
        CHECK(H5Aexists(dataset, "missing") == 0);
        attr.reset();
        test::Id reopened_attr(H5Aopen(dataset, "units", H5P_DEFAULT), H5Aclose);
        units_out.fill(-1);
        CHECK(H5Aread(reopened_attr, H5T_NATIVE_INT, units_out.data()) >= 0);
        CHECK(units_out == units);

        test::Id datatype(H5Tcopy(H5T_NATIVE_INT), H5Tclose);
        CHECK(H5Tcommit2(file, "integer", datatype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) >= 0);
        test::Id reopened_type(H5Topen2(file, "integer", H5P_DEFAULT), H5Tclose);
        CHECK(H5Tequal(reopened_type, H5T_NATIVE_INT) > 0);
        CHECK(H5Lexists(file, "group", H5P_DEFAULT) > 0);
        CHECK(H5Lexists(file, "missing", H5P_DEFAULT) == 0);
        test::Id reopened_group(H5Gopen2(file, "group", H5P_DEFAULT), H5Gclose);
        test::Id reopened_dataset(H5Dopen2(reopened_group, "values", H5P_DEFAULT), H5Dclose);
        selected.fill(-1);
        CHECK(H5Dread(reopened_dataset, H5T_NATIVE_INT, memory_space, file_space, H5P_DEFAULT, selected.data()) >= 0);
        CHECK(selected == replacement);
    });
}
