from ._lowfive import *

# Monkey-patch the constructor to accept normal mpi4py communicators

init = DistMetadataVOL.__init__
def convert_mpi_comm(self, local, intercomm):
    from mpi4py import MPI

    local = MPI._addressof(local)
    if type(intercomm) == list:
        intercomm = [MPI._addressof(comm) for comm in intercomm]
    else:
        intercomm = MPI._addressof(intercomm)

    init(self, local, intercomm)
DistMetadataVOL.__init__ = convert_mpi_comm
