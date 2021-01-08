#include <array>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>

#include <hdf5.h>

int main()
{
    // full space
    std::array<hsize_t, 3> cur { 16, 16, 16 },
                           max { 32, 32, 32 };
    hid_t full = H5Screate_simple(cur.size(), cur.data(), max.data());

    /* subset */
    std::array<hsize_t, 3> start  { 4, 4, 4 },
                           stride { 1, 1, 1 },
                           count  { 8, 8, 8 },
                           block  { 1, 1, 1 };

    int dim = 3;
    std::vector<hsize_t> min_(dim), max_(dim);
    H5Sget_select_bounds(full, min_.data(), max_.data());
    fmt::print("min = {}, max = {}\n", fmt::join(min_,  ","), fmt::join(max_,  ","));

    std::vector<hsize_t> dims_(dim), maxdims_(dim);
    H5Sget_simple_extent_dims(full, dims_.data(), maxdims_.data());
    fmt::print("dim = {}, max = {}\n", fmt::join(dims_,  ","), fmt::join(maxdims_,  ","));

    herr_t err = H5Sselect_hyperslab(full, H5S_SELECT_SET, start.data(), stride.data(), count.data(), block.data());
    if (err < 0)
        fmt::print("Error in hyperslab selection\n");
    fmt::print("Selected hyperslab\n");

    H5Sget_select_bounds(full, min_.data(), max_.data());
    fmt::print("min = {}, max = {}\n", fmt::join(min_,  ","), fmt::join(max_,  ","));

    H5Sget_simple_extent_dims(full, dims_.data(), maxdims_.data());
    fmt::print("dim = {}, max = {}\n", fmt::join(dims_,  ","), fmt::join(maxdims_,  ","));

    /* iterate */
    unsigned sel_iter_flags = 0;
    //sel_iter_flags = H5S_SEL_ITER_SHARE_WITH_DATASPACE;

    hid_t iter = H5Ssel_iter_create(full, 8, sel_iter_flags);

    std::vector<hsize_t> off(8);
    std::vector<size_t>  len(8);
    size_t nseq   = 0;
    size_t nbytes = 0;

    do
    {
        // len returned is in bytes (as the documentation says)
        // on the other hand nbytes returned seems to be in elements, contrary to
        // what the name might suggest
        H5Ssel_iter_get_seq_list (iter, off.size(), 1024*1024, &nseq, &nbytes, off.data(), len.data());

        fmt::print("nseq = {}, nbytes = {}\n", nseq, nbytes);

        for (size_t i = 0; i < nseq; ++i)
            fmt::print("off = {}, len = {}\n", off[i], len[i]);
    } while (nseq == off.size());

    H5Ssel_iter_close(iter);
}
