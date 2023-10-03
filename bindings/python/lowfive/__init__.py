from ._lowfive import *
from ._lowfive import _create_DistMetadataVOL, _create_MetadataVOL

import atexit

# vols      = []
# dist_vols = []

def create_MetadataVOL():
    print("before create vol in __init__")
    vol = _create_MetadataVOL()
    print("created vol in __init__")
    # vols.append(vol)
    return vol


def create_DistMetadataVOL(local, intercomm):
    vol = _create_DistMetadataVOL(local, intercomm)
    # dist_vols.append(vol)
    return vol

def unset_vol_callbacks():
    pass
    # for vol in dist_vols:
    #     vol.unset_dist_callbacks()
    #     vol.unset_callbacks()

    # for vol in vols:
    #     vol.unset_callbacks()

atexit.register(unset_vol_callbacks)

__all__ = ["create_DistMetadataVOL", "create_MetadataVOL", "create_logger", "DistMetatadataVOL", "MetadataVOL", "VOLBase"]
