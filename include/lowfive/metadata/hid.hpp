#pragma once

namespace LowFive
{

struct Hid
{
    hid_t   id;

            Hid(hid_t id_):         id(id_)         { if (id != 0) H5Iinc_ref(id); }

            Hid(const Hid& other):  id(other.id)    { if (id != 0) H5Iinc_ref(id); }
            Hid(Hid&& other):       id(other.id)    { if (id != 0) H5Iinc_ref(id); }

    Hid&    operator=(const Hid& other)             { id = other.id; if (id != 0) H5Iinc_ref(id); return *this; }
    Hid&    operator=(Hid&& other)                  { id = other.id; if (id != 0) H5Iinc_ref(id); return *this; }

            ~Hid()
    {
        hid_t err_id = H5Eget_current_stack();
        if (id != 0) H5Idec_ref(id);
        H5Eset_current_stack(err_id);
    }
};

}
