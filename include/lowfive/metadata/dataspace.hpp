#pragma once

namespace LowFive
{

struct Dataspace
{
    int                             dim;
    std::vector<size_t>             min;
    std::vector<size_t>             max;

            Dataspace(hid_t space_id)
    {
        dim = H5Sget_simple_extent_ndims(space_id);
        min.resize(dim);
        max.resize(dim);

        // TODO: add a check that the dataspace is simple

        std::vector<hsize_t> min_(dim), max_(dim);
        H5Sget_select_bounds(space_id, min_.data(), max_.data());

        for (size_t i = 0; i < dim; ++i)
        {
            min[i] = min_[i];
            max[i] = max_[i];
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const Dataspace& ds)
    {
        fmt::print(out, "(min = [{}], max = [{}])", fmt::join(ds.min, ","), fmt::join(ds.max, ","));
        return out;
    }
};

}
