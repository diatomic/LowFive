#include "test.hpp"
#include <lowfive/H5VOLProperty.hpp>
#include <memory>

namespace
{
void links(const std::string& operation)
{
    test::Connector connector("base-" + operation, "native");
    auto file = connector.file();
    test::Id group(H5Gcreate2(file, "source", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
    if (operation == "hard")
        CHECK(H5Lcreate_hard(file, "source", file, "target", H5P_DEFAULT, H5P_DEFAULT) >= 0);
    else if (operation == "hard-same-dst")
        CHECK(H5Lcreate_hard(file, "source", H5L_SAME_LOC, "target", H5P_DEFAULT, H5P_DEFAULT) >= 0);
    else if (operation == "hard-same-src")
        CHECK(H5Lcreate_hard(H5L_SAME_LOC, "source", file, "target", H5P_DEFAULT, H5P_DEFAULT) >= 0);
    else
    {
        H5VL_loc_params_t source{}, target{};
        source.obj_type = target.obj_type = H5I_FILE;
        source.type = target.type = H5VL_OBJECT_BY_NAME;
        source.loc_data.loc_by_name.name = "source";
        target.loc_data.loc_by_name.name = "target";
        source.loc_data.loc_by_name.lapl_id = target.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        using Wrapper = LowFive::VOLBase::pass_through_t;
        std::unique_ptr<Wrapper, void (*)(Wrapper*)> wrapper(
            new Wrapper(H5VLobject(file), H5VL_NATIVE, &connector.vol), Wrapper::destroy_wrapper);
        void* object = wrapper.get();
        CHECK(wrapper->under_object != nullptr);
        const bool null_source = operation.find("src") != std::string::npos;
        void* src = null_source ? nullptr : object;
        void* dst = null_source ? object : nullptr;
        // Enter the library state normally supplied by H5Lcopy/H5Lmove before
        // exercising nullable objects through the connector-author API.
        CHECK(H5VLstart_lib_state() >= 0);
        const herr_t result = operation.find("copy") == 0
            ? H5VLlink_copy(src, &source, dst, &target, connector.connector, H5P_LINK_CREATE_DEFAULT,
                            H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, nullptr)
            : H5VLlink_move(src, &source, dst, &target, connector.connector, H5P_LINK_CREATE_DEFAULT,
                            H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, nullptr);
        CHECK(H5VLfinish_lib_state() >= 0);
        CHECK(result >= 0);
        CHECK(H5Lexists(file, "source", H5P_DEFAULT) == (operation.find("copy") == 0 ? 1 : 0));
    }
    CHECK(H5Lexists(file, "target", H5P_DEFAULT) > 0);
    test::Id opened(H5Gopen2(file, "target", H5P_DEFAULT), H5Gclose);
}

void reopen(const std::string& mode)
{
    test::Connector connector("base-reopen-" + mode, mode);
    auto file = connector.file();
    test::Id group(H5Gcreate2(file, "before", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
    test::Id reopened(H5Freopen(file), H5Fclose);
    test::Id opened(H5Gopen2(reopened, "before", H5P_DEFAULT), H5Gclose);
    test::Id created(H5Gcreate2(reopened, "after", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
    CHECK(H5Lexists(file, "after", H5P_DEFAULT) > 0);
    opened.reset();
    created.reset();
    reopened.reset();
}

struct ThrowingVOL : LowFive::VOLBase
{
    void* group_create(void*, const H5VL_loc_params_t*, const char*, hid_t, hid_t, hid_t, hid_t, void**) override
    { throw std::runtime_error("group callback regression"); }
    herr_t link_specific(void*, const H5VL_loc_params_t*, hid_t, H5VL_link_specific_args_t*, hid_t, void**) override
    { throw 42; }
};

void exceptions()
{
    auto& vol = *new ThrowingVOL;
    const hid_t connector = vol.register_plugin();
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    CHECK(H5Pset_vol(fapl, connector, vol.info) >= 0);
    test::Id file(H5Fcreate("base-exceptions.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose);
    bool escaped = false;
    hid_t group = H5I_INVALID_HID;
    try { group = H5Gcreate2(file, "throws", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); }
    catch (...) { escaped = true; }
    CHECK(!escaped);
    CHECK(group < 0);
    CHECK(H5Eget_num(H5E_DEFAULT) > 0);
    H5Eclear2(H5E_DEFAULT);
    herr_t status = 0;
    try { status = H5Ldelete(file, "throws", H5P_DEFAULT); }
    catch (...) { escaped = true; }
    CHECK(!escaped);
    CHECK(status < 0);
    CHECK(H5Eget_num(H5E_DEFAULT) > 0);
    file.reset();
    fapl.reset();
    CHECK(H5VLunregister_connector(connector) >= 0);
    std::remove("base-exceptions.h5");
}

void property_lifetime()
{
    auto& vol = LowFive::VOLBase::create_vol_base();
    auto first = std::make_unique<LowFive::H5VOLProperty>(vol);
    auto second = std::make_unique<LowFive::H5VOLProperty>(vol);
    first.reset();
    CHECK(H5Iis_valid(second->vol_id) > 0);
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    second->apply(fapl);
    test::Id file(H5Fcreate("base-property.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose);
    second.reset();
    test::Id group(H5Gcreate2(file, "survivor", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
    group.reset();
    file.reset();
    fapl.reset();
    std::remove("base-property.h5");
}

void info_roundtrip(bool nested)
{
    test::Connector connector("base-info", "native");
    auto* original = LowFive::VOLBase::info;
    const std::string text = nested ? "under_vol=510;under_info={under_vol=0;under_info={}}" : "under_vol=0;under_info={}";
    void* parsed = nullptr;
    CHECK(H5VLconnector_str_to_info(text.c_str(), connector.connector, &parsed) >= 0);
    CHECK(parsed != nullptr);
    CHECK(parsed != original);
    CHECK(LowFive::VOLBase::info == original);
    const auto check_configuration = [nested](const void* value)
    {
        const auto* configuration = static_cast<const LowFive::VOLBase::info_t*>(value);
        CHECK(configuration != nullptr);
        H5VL_class_value_t underlying_value;
        CHECK(H5VLget_value(configuration->under_vol_id, &underlying_value) >= 0);
        CHECK(underlying_value == (nested ? 510 : 0));
        if (nested)
        {
            const auto* inner = static_cast<const LowFive::VOLBase::info_t*>(configuration->under_vol_info);
            CHECK(inner != nullptr);
            CHECK(H5VLget_value(inner->under_vol_id, &underlying_value) >= 0);
            CHECK(underlying_value == 0);
            CHECK(inner->under_vol_info == nullptr);
        }
        else CHECK(configuration->under_vol_info == nullptr);
    };
    check_configuration(parsed);
    char* serialized = nullptr;
    CHECK(H5VLconnector_info_to_str(parsed, connector.connector, &serialized) >= 0);
    CHECK(serialized != nullptr);
    void* decoded = nullptr;
    CHECK(H5VLconnector_str_to_info(serialized, connector.connector, &decoded) >= 0);
    CHECK(decoded != parsed);
    check_configuration(decoded);
    CHECK(H5VLfree_connector_info(connector.connector, decoded) >= 0);
    CHECK(H5free_memory(serialized) >= 0);
    CHECK(H5VLfree_connector_info(connector.connector, parsed) >= 0);
    auto file = connector.file();
    test::Id group(H5Gcreate2(file, "still-native", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
}

void malformed_info()
{
    test::Connector connector("base-info-invalid", "native");
    for (const char* text : {"", "under_vol=x;under_info={}", "under_vol=0;under_info={", "under_vol=0;under_info={}}", "under_vol=0;under_info={}trailing"})
    {
        void* parsed = nullptr;
        CHECK(H5VLconnector_str_to_info(text, connector.connector, &parsed) < 0);
        CHECK(parsed == nullptr);
    }
}
}

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        if (name == "reopen-native") reopen("native");
        else if (name == "reopen-metadata") reopen("file");
        else if (name == "exceptions") exceptions();
        else if (name == "property-lifetime") property_lifetime();
        else if (name == "info-independent") info_roundtrip(false);
        else if (name == "info-nested") info_roundtrip(true);
        else if (name == "info-malformed") malformed_info();
        else if (name == "singleton")
        {
            auto& first = LowFive::VOLBase::create_vol_base();
            auto& second = LowFive::VOLBase::create_vol_base();
            CHECK(&first == &second);
            const hid_t connector = first.register_plugin();
            CHECK(H5VLunregister_connector(connector) >= 0);
        }
        else links(name);
    });
}
