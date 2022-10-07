#pragma once

namespace LowFive
{

struct Hid
{
    hid_t   id = 0;

            Hid(hid_t id_, bool owned = false):
                id(id_)                             { if (id != 0 && !owned) H5Iinc_ref(id); }

            Hid(const Hid& other)                   { update(other.id); }
            Hid(Hid&& other)                        { update(other.id); }

    Hid&    operator=(const Hid& other)             { update(other.id); return *this; }
    Hid&    operator=(Hid&& other)                  { update(other.id); return *this; }

    void    update(hid_t oid)
    {
        if (oid != 0) H5Iinc_ref(oid);
        if (id != 0) H5Idec_ref(id);
        id = oid;
    }

            ~Hid()
    {
        // various H5*close() operations, under the hood, just decrement the reference
        // per the docs, once the reference count drops to 0, the respective object is closed
        // in particular, this means it's Ok to not call various close() functions directly
        hid_t err_id = H5Eget_current_stack();
        if (id != 0) H5Idec_ref(id);
        H5Eset_current_stack(err_id);
    }
};

}
