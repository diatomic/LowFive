#include <lowfive/vol-base.hpp>
#include <charconv>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <fmt/format.h>

LowFive::VOLBase::info_t* LowFive::VOLBase::info = nullptr;
LowFive::VOLBase* LowFive::VOLBase::info_t::vol = nullptr;

void*
LowFive::VOLBase::_info_copy(const void* source)
{
    const auto* original = static_cast<const info_t*>(source);
    auto copy = std::make_unique<info_t>();
    copy->under_vol_id = original->under_vol_id;
    copy->under_vol_info = nullptr;
    if (H5Iinc_ref(copy->under_vol_id) < 0)
        return nullptr;
    if (original->under_vol_info &&
        H5VLcopy_connector_info(copy->under_vol_id, &copy->under_vol_info, original->under_vol_info) < 0)
    {
        H5Idec_ref(copy->under_vol_id);
        return nullptr;
    }
    return copy.release();
}

herr_t
LowFive::VOLBase::_info_free(void* value)
{
    auto* owned = static_cast<info_t*>(value);
    if (!owned)
        return 0;
    const hid_t stack = H5Eget_current_stack();
    herr_t result = 0;
    if (owned->under_vol_info)
        result = H5VLfree_connector_info(owned->under_vol_id, owned->under_vol_info);
    if (H5Idec_ref(owned->under_vol_id) < 0)
        result = -1;
    delete owned;
    H5Eset_current_stack(stack);
    return result;
}

herr_t
LowFive::VOLBase::_info_to_str(const void* value, char** text)
{
    *text = nullptr;
    const auto* configuration = static_cast<const info_t*>(value);
    H5VL_class_value_t underlying_value;
    char* underlying_text = nullptr;
    if (H5VLget_value(configuration->under_vol_id, &underlying_value) < 0 ||
        H5VLconnector_info_to_str(configuration->under_vol_info, configuration->under_vol_id, &underlying_text) < 0)
        return -1;
    std::unique_ptr<char, decltype(&H5free_memory)> release(underlying_text, H5free_memory);
    const auto serialized = fmt::format("under_vol={};under_info={{{}}}",
                                       static_cast<unsigned>(underlying_value), underlying_text ? underlying_text : "");
    *text = static_cast<char*>(H5allocate_memory(serialized.size() + 1, false));
    if (!*text)
        return -1;
    std::memcpy(*text, serialized.c_str(), serialized.size() + 1);
    return 0;
}

herr_t
LowFive::VOLBase::_str_to_info(const char* text, void** result)
{
    *result = nullptr;
    if (!text)
        throw std::invalid_argument("null LowFive connector configuration");
    std::string_view encoded(text);
    if (!encoded.empty() && encoded.back() == ';')
        encoded.remove_suffix(1); // HDF5_VOL_CONNECTOR configurations may end in ';'.
    constexpr std::string_view prefix = "under_vol=";
    constexpr std::string_view separator = ";under_info={";
    const auto split = encoded.find(separator);
    if (encoded.substr(0, prefix.size()) != prefix || split == std::string_view::npos ||
        encoded.empty() || encoded.back() != '}')
        throw std::invalid_argument("invalid LowFive connector configuration");
    unsigned underlying_value = 0;
    const char* number_begin = encoded.data() + prefix.size();
    const char* number_end = encoded.data() + split;
    const auto number = std::from_chars(number_begin, number_end, underlying_value);
    if (number.ec != std::errc{} || number.ptr != number_end)
        throw std::invalid_argument("invalid underlying connector value");
    const auto payload = encoded.substr(split + separator.size(), encoded.size() - split - separator.size() - 1);
    size_t depth = 0;
    for (char character : payload)
    {
        if (character == '{') ++depth;
        else if (character == '}')
        {
            if (depth == 0)
                throw std::invalid_argument("unbalanced underlying connector configuration");
            --depth;
        }
    }
    if (depth != 0)
        throw std::invalid_argument("unbalanced underlying connector configuration");
    // std::string owns the payload and its terminator, including for nested info.
    const std::string underlying_text(payload);
    auto configuration = std::make_unique<info_t>();
    configuration->under_vol_info = nullptr;
    configuration->under_vol_id = H5VLregister_connector_by_value(
        static_cast<H5VL_class_value_t>(underlying_value), H5P_DEFAULT);
    if (configuration->under_vol_id < 0)
        return -1;
    if (!payload.empty() && H5VLconnector_str_to_info(underlying_text.c_str(), configuration->under_vol_id,
                                                    &configuration->under_vol_info) < 0)
    {
        H5Idec_ref(configuration->under_vol_id);
        return -1;
    }
    *result = configuration.release();
    return 0;
}
