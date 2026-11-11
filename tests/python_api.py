import ctypes
import gc
import sys

import lowfive
from lowfive import _lowfive


def exports():
    namespace = {}
    exec("from lowfive import *", namespace)
    assert namespace["DistMetadataVOL"] is lowfive.DistMetadataVOL
    assert namespace["MetadataVOL"] is lowfive.MetadataVOL
    assert namespace["VOLBase"] is lowfive.VOLBase


def errors():
    # Importing LowFive must not turn a recoverable HDF5 error into exit(1).
    hdf5 = ctypes.CDLL(_lowfive.__file__)
    hdf5.H5Fopen.argtypes = [ctypes.c_char_p, ctypes.c_uint, ctypes.c_longlong]
    hdf5.H5Fopen.restype = ctypes.c_longlong
    assert hdf5.H5Fopen(b"no-such-lowfive-python-input.h5", 0, 0) < 0


def lifetime():
    hdf5 = ctypes.CDLL(_lowfive.__file__)
    hdf5.H5VLregister_connector_by_name.argtypes = [ctypes.c_char_p, ctypes.c_longlong]
    hdf5.H5VLregister_connector_by_name.restype = ctypes.c_longlong
    assert hdf5.H5VLregister_connector_by_name(b"lowfive", 0) >= 0
    vol = lowfive.create_VOLBase()
    del vol
    gc.collect()
    # HDF5 owns the singleton, not the Python wrapper. Reuse and final shutdown
    # must remain valid after collecting the first wrapper.
    vol = lowfive.create_VOLBase()
    # Connector termination releases the singleton even while a wrapper exists.
    hdf5.H5close.restype = ctypes.c_int
    assert hdf5.H5close() >= 0
    del vol
    gc.collect()


{"exports": exports, "errors": errors, "lifetime": lifetime}[sys.argv[1]]()
