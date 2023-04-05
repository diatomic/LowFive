from ._lowfive import *
from ._lowfive import _create_DistMetadataVOL, _create_MetadataVOL

import atexit

vols = []

def create_MetadataVOL():
    vol = _create_MetadataVOL()
    vols.append(vol)
    return vol


def create_DistMetadataVOL(local, intercomm):
    vol = _create_DistMetadataVOL(local, intercomm)
    vols.append(vol)
    return vol

def unset_vol_callbacks():
    for vol in vols:
        vol.unset_callbacks()

atexit.register(unset_vol_callbacks)

__all__ = ["create_DistMetadataVOL", "create_MetadataVOL", "create_logger", "DistMetatadataVOL", "MetadataVOL", "VOLBase"]
