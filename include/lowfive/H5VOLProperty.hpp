#pragma once

#include "vol-base.hpp"
#include <stdexcept>

namespace LowFive
{

struct H5VOLProperty
{
    H5VOLProperty(VOLBase& vol_plugin_):
        vol_plugin(vol_plugin_)
    {
        vol_id = vol_plugin.register_plugin();
        if (vol_id < 0)
            throw std::runtime_error("Failed to register the LowFive VOL connector");
    }
    ~H5VOLProperty() { H5VLunregister_connector(vol_id); }

    H5VOLProperty(const H5VOLProperty&) = delete;
    H5VOLProperty& operator=(const H5VOLProperty&) = delete;
    H5VOLProperty(H5VOLProperty&&) = delete;
    H5VOLProperty& operator=(H5VOLProperty&&) = delete;

    void apply(hid_t list) const
    {
        if (H5Pset_vol(list, vol_id, vol_plugin.info) < 0)
            throw std::runtime_error("Failed to apply the LowFive VOL connector");
    }

    hid_t       vol_id;
    VOLBase&    vol_plugin;
};

}
