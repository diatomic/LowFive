#pragma once

namespace LowFive
{

struct Hid
{
    hid_t   id;

            Hid(hid_t id_, bool owned = false):
                id(id_)                             { if (id != 0 && !owned) H5Iinc_ref(id); }

            Hid(const Hid& other):  id(other.id)    { if (id != 0) H5Iinc_ref(id); }
            Hid(Hid&& other):       id(other.id)    { if (id != 0) H5Iinc_ref(id); }

    Hid&    operator=(const Hid& other)             { id = other.id; if (id != 0) H5Iinc_ref(id); return *this; }
    Hid&    operator=(Hid&& other)                  { id = other.id; if (id != 0) H5Iinc_ref(id); return *this; }

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
