#pragma once
#include <diy/serialization.hpp>

namespace diy
{

template<>
struct Serialization<::LowFive::Dataspace>
{
    using Dataspace = ::LowFive::Dataspace;

    static void         save(BinaryBuffer& bb, const Dataspace& ds)
    {
        // encode the destination space
        size_t nalloc = 0;
        H5Sencode(ds.id, NULL, &nalloc, 0);                 // first time sets nalloc to the required size
        diy::save(bb, nalloc);
        H5Sencode(ds.id, bb.grow(nalloc), &nalloc, 0);      // second time actually encodes the data
    }

    static void         load(BinaryBuffer& bb, Dataspace& ds)
    {
        size_t nalloc;
        diy::load(bb, nalloc);
        hid_t ds_id = H5Sdecode(bb.advance(nalloc));

        ds = Dataspace(ds_id, true);
    }
};

template<>
struct Serialization<::LowFive::Datatype>
{
    using Datatype = ::LowFive::Datatype;

    static void         save(BinaryBuffer& bb, const Datatype& dt)
    {
        // encode the destination space
        size_t nalloc = 0;
        H5Tencode(dt.id, NULL, &nalloc);                  // first time sets nalloc to the required size
        diy::save(bb, nalloc);
        H5Tencode(dt.id, bb.grow(nalloc), &nalloc);       // second time actually encodes the data
    }

    static void         load(BinaryBuffer& bb, Datatype& dt)
    {
        size_t nalloc;
        diy::load(bb,nalloc);
        hid_t dt_id = H5Tdecode(bb.advance(nalloc));

        dt = Datatype(dt_id, true);
    }
};

}
