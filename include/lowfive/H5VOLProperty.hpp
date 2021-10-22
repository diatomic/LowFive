#pragma once

#include "vol-base.hpp"
#include <cassert>

namespace LowFive
{

struct H5VOLProperty
{
            H5VOLProperty(VOLBase& vol_plugin_):
                vol_plugin(vol_plugin_)
    {
        vol_id = vol_plugin.register_plugin();
    }
            ~H5VOLProperty()
    {
        H5VLterminate(vol_id);
        H5VLunregister_connector(vol_id);
        assert(H5VLis_connector_registered_by_name(vol_plugin.name.c_str()) == 0);
    }

    void    apply(const hid_t list) const   { H5Pset_vol(list, vol_id, vol_plugin.info); }

    hid_t       vol_id;
    VOLBase&    vol_plugin;
};

}
