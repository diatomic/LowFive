#pragma once

namespace LowFive
{

struct Dataspace: Hid
{
    enum class SelectionType { none, points, hyperslabs, all };

    int                             dim;
    std::vector<size_t>             min, max,
                                    dims, maxdims;

    SelectionType                   selection;

    // hyperslab
    std::vector<size_t>             start, stride, count, block;

            Dataspace(hid_t space_id):
                Hid(space_id)
    {
        if (id == 0)
            return;

        dim = H5Sget_simple_extent_ndims(id);
        min.resize(dim);
        max.resize(dim);
        dims.resize(dim);
        maxdims.resize(dim);

        std::vector<hsize_t> min_(dim), max_(dim);
        H5Sget_select_bounds(id, min_.data(), max_.data());

        std::vector<hsize_t> dims_(dim), maxdims_(dim);
        H5Sget_simple_extent_dims(space_id, dims_.data(), maxdims_.data());


        H5S_sel_type selection_ = H5Sget_select_type(space_id);
        if (selection_ == H5S_SEL_NONE)
        {
            selection = SelectionType::none;
        } else if (selection_ == H5S_SEL_POINTS)
        {
            selection = SelectionType::points;
        } else if (selection_ == H5S_SEL_HYPERSLABS)
        {
            selection = SelectionType::hyperslabs;

            if (H5Sis_regular_hyperslab(space_id) <= 0)
                throw MetadataError(fmt::format("Cannot handle irregular hyperslabs, space_id = {}", space_id));

            start.resize(dim);
            stride.resize(dim);
            count.resize(dim);
            block.resize(dim);

            std::vector<hsize_t> start_(dim), stride_(dim), count_(dim), block_(dim);
            H5Sget_regular_hyperslab(space_id, start_.data(), stride_.data(), count_.data(), block_.data());

            for (size_t i = 0; i < dim; ++i)
            {
                start[i]    = start_[i];
                stride[i]   = stride_[i];
                count[i]    = count_[i];
                block[i]    = block_[i];
            }
        } else if (selection_ == H5S_SEL_ALL) {
            selection = SelectionType::all;
        }

        for (size_t i = 0; i < dim; ++i)
        {
            min[i]      = min_[i];
            max[i]      = max_[i];
            dims[i]     = dims_[i];
            maxdims[i]  = maxdims_[i];
        }
    }

    hid_t   copy() const
    {
        return H5Scopy(id);
    }

    friend std::ostream& operator<<(std::ostream& out, const Dataspace& ds)
    {
        fmt::print(out, "(min = [{}], max = [{}], dims = [{}], maxdims = [{}]",
                   fmt::join(ds.min,    ","),
                   fmt::join(ds.max,    ","),
                   fmt::join(ds.dims,   ","),
                   fmt::join(ds.maxdims, ","));

        fmt::print(out, ", selection = ");
        if (ds.selection == SelectionType::none)
            fmt::print(out, "none");
        else if (ds.selection == SelectionType::points)
            fmt::print(out, "points");
        else if (ds.selection == SelectionType::hyperslabs)
        {
            fmt::print(out, "hyperslab = (");
            fmt::print(out, "start = [{}], stride = [{}], count = [{}], block = [{}]",
                       fmt::join(ds.start,  ","),
                       fmt::join(ds.stride, ","),
                       fmt::join(ds.count,  ","),
                       fmt::join(ds.block,  ","));
            fmt::print(out, ")");
        }
        else if (ds.selection == SelectionType::all)
            fmt::print(out, "all");

        fmt::print(out, ")");

        return out;
    }
};

}
