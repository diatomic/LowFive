# LowFive

LowFive is a data transport layer based on the [HDF5](https://www.hdfgroup.org/solutions/hdf5/) data model, for in situ
workflows. Executables using LowFive can communicate in situ (using in-memory
data and MPI message passing), reading and writing traditional HDF5 files to
physical storage, and combining the two modes. Minimal and often no source-code
modification is needed for programs that already use HDF5. LowFive maintains
deep copies or shallow references of datasets, configurable by the user. More
than one task can produce (write) data, and more than one task can consume
(read) data, accommodating fan-in and fan-out in the workflow task graph.
LowFive supports data redistribution from n producer processes to m consumer
processes.

## Connector behavior

Missing object, group, and attribute locations return HDF5 errors. Unsupported
metadata-only optional operations also return errors rather than reporting
success; optional-operation queries advertise only implemented operations.
Native-backed objects continue to forward operations to the underlying connector.

## Testing

Build with MPI and HDF5 1.14, including its high-level library:

```sh
cmake -S . -B build -DHDF5_ROOT=/path/to/hdf5 -Dlowfive_python=OFF
cmake --build build
ctest --test-dir build --output-on-failure
```

The isolated API tests cover storage modes, datatype conversion, selections,
attributes, links and paths, object lifetimes, VOL wrapping, connector
configuration, distributed queries, and RPC teardown. Checks remain enabled in
Release builds. The MPI matrix additionally exercises redistribution between
one or two producers and consumers.

Use CTest labels to select a layer:

```sh
ctest --test-dir build --output-on-failure -L api
ctest --test-dir build --output-on-failure -L mpi
ctest --test-dir build --output-on-failure -L python
ctest --test-dir build --output-on-failure -L install
```

Python tests are enabled with `lowfive_python`. The distributed Python example
is registered when the selected interpreter provides mpi4py, h5py, and NumPy.
Each connector test runs in its own process because LowFive maintains one
active connector instance per process.
