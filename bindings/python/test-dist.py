import sys

memory = False
if '-m' in sys.argv:
    memory = True
    sys.argv.remove('-m')
file = False
if '-f' in sys.argv:
    file = True
    sys.argv.remove('-f')
if not file and not memory:
    print("Neither -m, nor -f specified. Forcing -f")
    file = True

import lowfive
from mpi4py import MPI

lowfive.create_logger("debug")

world = MPI.COMM_WORLD
size  = world.Get_size()
rank  = world.Get_rank()
if size < 2:
    raise RuntimeError("test-dist.py requires at least two MPI ranks")

producer_ranks = size // 2
producer = rank < producer_ranks

local = world.Split(producer)
intercomm = local.Create_intercomm(0, world, 0 if not producer else producer_ranks, 0)

vol = lowfive.create_DistMetadataVOL(local, intercomm)
if file:
    vol.set_passthru("*","*")
if memory:
    vol.set_memory("*","*")
    vol.set_intercomm("*", "*", 0)
# vol.set_keep(True)


# The actual I/O

import numpy as np
import h5py
def write_dataset(mode):
    with h5py.File("out.h5", mode) as f:
        f.create_dataset(f"data{local.rank}", data=np.ones((4, 3, 2), 'f'))
        vol.print_files()


if producer:
    print(f"Producer: {local.rank} out of {local.size}")
    if file:
        # Serial HDF5 cannot safely truncate one file from multiple processes.
        # Create it once, then let each remaining producer append its dataset.
        for writer in range(local.size):
            if local.rank == writer:
                write_dataset("w" if writer == 0 else "a")
            local.Barrier()
    else:
        write_dataset("w")

    if file:
        intercomm.Barrier()
else:
    if file:
        intercomm.Barrier()

    print(f"Consumer: {local.rank} out of {local.size}")
    with h5py.File("out.h5", "r") as f:
        data = f[f"data{local.rank % producer_ranks}"]
        assert np.all(data[...] == 1)
        print("Consumer:", data)

# Exercise Python wrappers after HDF5 has destroyed the connector singleton.
# Both callback cleanup methods must treat the stale wrapper as a no-op.
world.Barrier()
import ctypes
from lowfive import _lowfive
hdf5 = ctypes.CDLL(_lowfive.__file__)
hdf5.H5close.restype = ctypes.c_int
assert hdf5.H5close() >= 0
vol.unset_dist_callbacks()
vol.unset_callbacks()
