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

## Vendored Python tooling

The vendored pybind11 documentation and test manifests contain local security
updates. These are development dependencies, not LowFive runtime dependencies.
Documentation tooling requires Python 3.10 or newer. The primary pybind11 test
manifest provides NumPy/SciPy environments for CPython 3.10–3.13; the older copy
inside DIY uses CPython 3.10 or 3.11. Library interpreter support is unchanged.

To regenerate the documentation lockfile from `ext/pybind11`, run:

```sh
uv pip compile --python-version 3.10 docs/requirements.in -o docs/requirements.txt
```

Retain the local security constraints when refreshing vendored sources with peru.
