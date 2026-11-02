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

In-memory datasets apply their configured HDF5 fill value to unwritten selected
elements, including distributed reads and datatype conversion. Unselected
destination elements remain unchanged. Newly created, unwritten attributes read
as zero-initialized values in the requested memory datatype.
Creation-property queries depend only on dataset metadata, not obsolete stored
writes. For datatypes without variable-length members, returned property-list
copies keep modern-reference fill values alive; references obtained with
`H5Pget_fill_value` are borrowed and require `H5Rcopy` before being owned or
destroyed independently.

Scalar and null datasets accept explicit HDF5 dataspaces as well as `H5S_ALL`.
Empty selections leave the destination buffer unchanged.

Missing object, group, and attribute locations return HDF5 errors. Unsupported
metadata-only optional operations also return errors rather than reporting
success; optional-operation queries advertise only implemented operations.
Native-backed objects continue to forward operations to the underlying connector.
Open-object counts and ID lists are scoped to the requested file;
`H5F_OBJ_LOCAL` restricts them to the originating file handle.

Asynchronous underlying connectors are supported through request wait, notify,
cancel, specific, optional, and free callbacks. Multi-dataset submissions retain
each underlying request and share the caller's wait timeout across the aggregate.
Connector-specific optional request operations can be forwarded for a single
request, but are unsupported for aggregates because their outputs have no generic
merge contract. Failed submissions poll accepted work and cancel pending
operations before releasing ownership. If the underlying connector fails both
wait and cancellation without establishing completion, LowFive reports an error
and retains the unreleasable request rather than freeing an operation that may
still be active.

`broadcast_files()` replaces the receiver's metadata snapshot. Existing handles
keep their previous snapshot and its same-file reference targets until closed;
subsequent opens use the replacement.
Repeated broadcasts, including an empty snapshot, retire prior map ownership.
The metadata wire format carries unwritten attribute state and dataset fill
values; communicating processes must use the same LowFive version. Distributed
modern-reference values remain unsupported.

Distributed communicator indices must be nonnegative and smaller than the
configured communicator count. Invalid indices in mappings, direct calls, or
`set_serve_indices` callback results throw `std::out_of_range` in C++ and raise
`IndexError` in Python before entering MPI communication.

`serve_all(delete_data=true)` retires the file snapshots and their stored writes
after serving completes and the redistribution index is released, even when
`keep` is enabled. Existing producer handles remain valid until closed; their
snapshots are released when the last handle closes. Pass `delete_data=false` to
retain snapshots for another serving round. Automatic serving on file close uses
the default cleanup behavior.

## Testing

Build with MPI and HDF5 1.14, including its high-level library:

```sh
cmake -S . -B build -DHDF5_ROOT=/path/to/hdf5 -Dlowfive_python=OFF
cmake --build build
ctest --test-dir build --output-on-failure
```

The isolated API tests cover storage modes, datatype conversion, selections,
attributes, links and paths, object lifetimes, VOL wrapping, connector
configuration, asynchronous request lifecycles, distributed queries, and RPC
teardown. Regression coverage includes missing locations, unsupported operations,
file-scoped IDs, unwritten attributes, fill-value serialization, partial remote
reads, and snapshot replacement. Checks remain enabled in Release builds. The
MPI matrix additionally exercises redistribution between one or two producers
and consumers.

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
