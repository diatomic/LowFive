#pragma once

namespace LowFive
{

struct Dataspace: Hid
{
    enum class Class     { scalar, simple, null };
    enum class Selection { none, points, hyperslabs, all };

    int                             dim;
    std::vector<size_t>             min, max,
                                    dims, maxdims;

    Class                           cls;
    Selection                       selection;

    // hyperslab
    std::vector<size_t>             start, stride, count, block;

            Dataspace(hid_t space_id, bool owned = false):
                Hid(space_id, owned)
    {
        if (id == 0)
            return;

        H5S_class_t cls_ = H5Sget_simple_extent_type(id);

        if (cls_ == H5S_NO_CLASS)
            throw MetadataError(fmt::format("Unexpected no class in dataspace = {}", id));
        else if (cls_ == H5S_SCALAR)
            cls = Class::scalar;
        else if (cls_ == H5S_SIMPLE)
            cls = Class::simple;
        else if (cls_ == H5S_NULL)
            cls = Class::null;

        if (cls != Class::simple)
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
            selection = Selection::none;
        } else if (selection_ == H5S_SEL_POINTS)
        {
            selection = Selection::points;
        } else if (selection_ == H5S_SEL_HYPERSLABS)
        {
            selection = Selection::hyperslabs;

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
            selection = Selection::all;
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

    static hid_t project_intersection(hid_t src_space_id, hid_t dst_space_id, hid_t src_intersect_space_id)
    {
        return H5Sselect_project_intersection(src_space_id, dst_space_id, src_intersect_space_id);
    }

    friend std::ostream& operator<<(std::ostream& out, const Dataspace& ds)
    {
        if (ds.cls == Class::scalar)
            fmt::print(out, "class = scalar");
        else if (ds.cls == Class::null)
            fmt::print(out, "class = null");
        else if (ds.cls == Class::simple)
        {
            fmt::print(out, "class = simple, ");

            fmt::print(out, "(min = [{}], max = [{}], dims = [{}], maxdims = [{}]",
                       fmt::join(ds.min,    ","),
                       fmt::join(ds.max,    ","),
                       fmt::join(ds.dims,   ","),
                       fmt::join(ds.maxdims, ","));

            fmt::print(out, ", selection = ");
            if (ds.selection == Selection::none)
                fmt::print(out, "none");
            else if (ds.selection == Selection::points)
                fmt::print(out, "points");
            else if (ds.selection == Selection::hyperslabs)
            {
                fmt::print(out, "hyperslab = (");
                fmt::print(out, "start = [{}], stride = [{}], count = [{}], block = [{}]",
                           fmt::join(ds.start,  ","),
                           fmt::join(ds.stride, ","),
                           fmt::join(ds.count,  ","),
                           fmt::join(ds.block,  ","));
                fmt::print(out, ")");
            }
            else if (ds.selection == Selection::all)
                fmt::print(out, "all");

            fmt::print(out, ")");
        }

        return out;
    }
};

}
