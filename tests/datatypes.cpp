#include "test.hpp"
#include <algorithm>
#include <vector>

namespace
{
void binary()
{
    test::Connector connector("datatype-binary");
    auto file = connector.file();
    test::Id type(H5Tcopy(H5T_NATIVE_INT), H5Tclose);
    CHECK(H5Tcommit2(file, "type", type, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) >= 0);
    void* object = H5VLobject(type);
    CHECK(object != nullptr);
    H5VL_datatype_get_args_t args {};
    size_t size = 0;
    args.op_type = H5VL_DATATYPE_GET_BINARY_SIZE;
    args.args.get_binary_size.size = &size;
    CHECK(connector.vol.datatype_get(object, &args, H5P_DEFAULT, nullptr) >= 0);
    CHECK(size > 1);

    constexpr unsigned char guard = 0xa5;
    std::vector<unsigned char> bytes(size + 64, guard);
    args.op_type = H5VL_DATATYPE_GET_BINARY;
    args.args.get_binary.buf = bytes.data();
    args.args.get_binary.buf_size = bytes.size();
    CHECK(connector.vol.datatype_get(object, &args, H5P_DEFAULT, nullptr) >= 0);
    CHECK(std::all_of(bytes.begin() + size, bytes.end(),
                      [](unsigned char byte) { return byte == guard; }));
    test::Id decoded(H5Tdecode(bytes.data()), H5Tclose);
    CHECK(H5Tequal(decoded, H5T_NATIVE_INT) > 0);

    std::fill(bytes.begin(), bytes.end(), guard);
    args.args.get_binary.buf_size = size - 1;
    CHECK(connector.vol.datatype_get(object, &args, H5P_DEFAULT, nullptr) >= 0);
    CHECK(args.args.get_binary.buf_size == size);
    CHECK(std::all_of(bytes.begin(), bytes.end(),
                      [](unsigned char byte) { return byte == guard; }));
    args.args.get_binary.buf_size = size;
    CHECK(connector.vol.datatype_get(object, &args, H5P_DEFAULT, nullptr) >= 0);
    test::Id retried(H5Tdecode(bytes.data()), H5Tclose);
    CHECK(H5Tequal(retried, H5T_NATIVE_INT) > 0);

    args.args.get_binary.buf = nullptr;
    args.args.get_binary.buf_size = 0;
    CHECK(connector.vol.datatype_get(object, &args, H5P_DEFAULT, nullptr) >= 0);
    CHECK(args.args.get_binary.buf_size == size);
}

void object_info()
{
    test::Connector connector("datatype-object-info");
    auto file = connector.file();
    test::Id type(H5Tcopy(H5T_NATIVE_INT), H5Tclose);
    CHECK(H5Tcommit2(file, "type", type, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) >= 0);
    auto space = test::scalar();
    test::Id attribute(H5Acreate2(type, "units", H5T_NATIVE_INT, space,
                                  H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    H5O_info2_t direct {}, named {}, indexed {};
    CHECK(H5Oget_info3(type, &direct, H5O_INFO_BASIC | H5O_INFO_NUM_ATTRS) >= 0);
    CHECK(H5Oget_info_by_name3(file, "type", &named,
                              H5O_INFO_BASIC | H5O_INFO_NUM_ATTRS, H5P_DEFAULT) >= 0);
    CHECK(H5Oget_info_by_idx3(file, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &indexed,
                             H5O_INFO_BASIC | H5O_INFO_NUM_ATTRS, H5P_DEFAULT) >= 0);
    for (const auto* info : {&direct, &named, &indexed})
    {
        CHECK(info->type == H5O_TYPE_NAMED_DATATYPE);
        CHECK(info->num_attrs == 1);
        int comparison = 1;
        CHECK(H5Otoken_cmp(file, &direct.token, &info->token, &comparison) >= 0);
        CHECK(comparison == 0);
    }
    H5O_type_t reference_type = H5O_TYPE_UNKNOWN;
    H5R_ref_t reference {};
    CHECK(H5Rcreate_object(file, "type", H5P_DEFAULT, &reference) >= 0);
    const herr_t status = H5Rget_obj_type3(&reference, H5P_DEFAULT, &reference_type);
    CHECK(H5Rdestroy(&reference) >= 0);
    CHECK(status >= 0);
    CHECK(reference_type == H5O_TYPE_NAMED_DATATYPE);
}
}

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        if (name == "binary") binary();
        else if (name == "object-info") object_info();
        else CHECK(false);
    });
}
