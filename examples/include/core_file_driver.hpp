#include <hdf5.h>
#include <highfive/H5FileDriver.hpp>

class CoreFileAccess
{
    public:
        CoreFileAccess(size_t incr)
            : _incr(incr)
        {}

        void apply(const hid_t list) const
        {
            // debug
            fprintf(stderr, "CoreFileAccess applied\n");

            if (H5Pset_fapl_core(list, _incr, 0))
                HighFive::HDF5ErrMapper::ToException<HighFive::FileException>("Unable to setup Core Driver configuration");
        }
    private:
        size_t _incr;               // increment by which to grow memory (bytes)
};

class CoreFileDriver : public HighFive::FileDriver
{
    public:
        inline CoreFileDriver(size_t incr);

    private:
};

inline CoreFileDriver::CoreFileDriver(size_t incr)
{
    // debug
    fprintf(stderr, "CoreFileDriver\n");

    add(CoreFileAccess(incr));
}
