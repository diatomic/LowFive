#pragma once

#include <cassert>

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

            Dataspace(hid_t space_id = 0, bool owned = false):
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

            for (int i = 0; i < dim; ++i)
            {
                start[i]    = start_[i];
                stride[i]   = stride_[i];
                count[i]    = count_[i];
                block[i]    = block_[i];
            }
        } else if (selection_ == H5S_SEL_ALL) {
            selection = Selection::all;
        }

        for (int i = 0; i < dim; ++i)
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

    size_t  size() const
    {
        return H5Sget_simple_extent_npoints(id);
    }

    void    set_extent(const hsize_t* size, const hsize_t* maxsize = nullptr)
    {
        assert(cls == Class::simple);

        if (maxsize)
        {
            for (auto i = 0; i < dim; i++)
                maxdims[i] = maxsize[i];
        }

        for (auto i = 0; i < dim; i++)
        {
            if (maxdims[i] == H5S_UNLIMITED || maxdims[i] >= size[i])   // ensure size does not exceed max allowed
            {
                dims[i] = size[i];                                      // update size
                max[i]  = min[i] + dims[i] - 1;                         // update max to reflect new size, keeping min original
            }
            else
                throw MetadataError(fmt::format("Dataspace::set_extent(): trying to set size larger than maxdims in dimension {}", i));
        }

        // update HDF5's internal representation
        if (maxsize)
            H5Sset_extent_simple(id, dim, size, maxsize);
        else
        {
            std::vector<hsize_t> maxdims(dim);
            for (auto i = 0; i < dim; i++)
                maxdims[i] = this->maxdims[i];  // copying size_t to hsize_t
            H5Sset_extent_simple(id, dim, size, maxdims.data());
        }
    }

    bool    intersects(const Dataspace& other) const
    {
        std::vector<hsize_t> start, end;
        for (int i = 0; i < other.dim; ++i)
        {
            start.push_back(other.min[i]);
            end.push_back(other.max[i]);
        }
        return H5Sselect_intersect_block(id, start.data(), end.data()) > 0;
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

    // operate on entire sequence in bulk
    template<class F>
    static void iterate(const   Dataspace& space,
                        size_t  element_size,
                        const   F& f)
    {
        unsigned sel_iter_flags = 0;
        //sel_iter_flags = H5S_SEL_ITER_SHARE_WITH_DATASPACE;

        hid_t iter = H5Ssel_iter_create(space.id, element_size, sel_iter_flags);

        std::vector<hsize_t> off(1024);
        std::vector<size_t>  len(1024);         // len returned is in bytes (as the documentation says)
        size_t nseq   = 0;
        size_t nbytes = 0;                      // this is a misnomer; I believe the returned value is in elements

        do
        {
            H5Ssel_iter_get_seq_list (iter, off.size(), 1024*1024, &nseq, &nbytes, off.data(), len.data());

            for (size_t i = 0; i < nseq; ++i)
                f(off[i], len[i]);              // call f(size_t loc, size_t len)
        } while (nseq);

        H5Ssel_iter_close(iter);
    }

    // operate on longest length sequences possible in bulk
    template<class F>
    static void iterate(const   Dataspace& space1, size_t size1,
                        const   Dataspace& space2, size_t size2,
                        const   F& f)
    {
        unsigned sel_iter_flags = 0;
        //sel_iter_flags = H5S_SEL_ITER_SHARE_WITH_DATASPACE;

        hid_t iter1 = H5Ssel_iter_create(space1.id, size1, sel_iter_flags);
        hid_t iter2 = H5Ssel_iter_create(space2.id, size2, sel_iter_flags);

        std::vector<hsize_t> off1(1024);
        std::vector<size_t>  len1(1024);     // len returned is in bytes (as the documentation says)
        size_t nseq1   = 0;
        size_t nbytes1 = 0;                  // this is a misnomer; I believe the returned value is in elements

        std::vector<hsize_t> off2(1024);
        std::vector<size_t>  len2(1024);     // len returned is in bytes (as the documentation says)
        size_t nseq2   = 0;
        size_t nbytes2 = 0;                  // this is a misnomer; I believe the returned value is in elements

        bool done1 = false, done2 = false;
        size_t i = 0, j = 0;
        size_t loc1 = 0, loc2 = 0;
        while(!done1 && !done2)
        {
            if (!done1 && i == nseq1)
            {
                H5Ssel_iter_get_seq_list (iter1, off1.size(), 1024*1024, &nseq1, &nbytes1, off1.data(), len1.data());
                done1 = (nseq1 == 0);
                i = 0;
                loc1 = 0;
            }
            if (!done2 && j == nseq2)
            {
                H5Ssel_iter_get_seq_list (iter2, off2.size(), 1024*1024, &nseq2, &nbytes2, off2.data(), len2.data());
                done2 = (nseq2 == 0);
                j = 0;
                loc2 = 0;
            }

            while (i < nseq1 && j < nseq2)
            {
                while (loc1 < len1[i] && loc2 < len2[j])
                {
                    size_t min_len = std::min(len1[i] - loc1, len2[j] - loc2);
                    f(off1[i] + loc1, off2[j] + loc2, min_len);     // call f(size_t loc1, size_t loc2, size_t len)

                    loc1 += min_len;
                    loc2 += min_len;
                }

                if (loc1 >= len1[i])
                {
                    ++i;
                    loc1 = 0;
                }
                if (loc2 >= len2[j])
                {
                    ++j;
                    loc2 = 0;
                }
            }
        }

        H5Ssel_iter_close(iter2);
        H5Ssel_iter_close(iter1);
    }
};

}
