#pragma once

#include "vol-base.hpp"

namespace LowFive
{

struct H5VOLProperty
{
            H5VOLProperty(VOLBase& vol_plugin_):
                vol_plugin(vol_plugin_)
    {
        vol_id = vol_plugin.register_plugin();
    }
            ~H5VOLProperty() { H5VLunregister_connector(vol_id); }

            H5VOLProperty(const H5VOLProperty&) = delete;
            H5VOLProperty& operator=(const H5VOLProperty&) = delete;
            H5VOLProperty(H5VOLProperty&&) = delete;
            H5VOLProperty& operator=(H5VOLProperty&&) = delete;

    void    apply(const hid_t list) const   { H5Pset_vol(list, vol_id, vol_plugin.info); }

    hid_t       vol_id;
    VOLBase&    vol_plugin;
};

}
