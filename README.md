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

## Testing

Build with MPI and HDF5 1.14, including its high-level library:

```sh
cmake -S . -B build -DHDF5_ROOT=/path/to/hdf5 -Dlowfive_python=OFF
cmake --build build
ctest --test-dir build --output-on-failure
```

The API tests check data, selections, attributes, and named datatypes in memory,
file, combined, and native-passthrough modes. Each case runs in its own process
because LowFive maintains a single active connector. Checks remain enabled in
Release builds. The existing MPI matrix also exercises redistribution between
one or two producers and consumers.
