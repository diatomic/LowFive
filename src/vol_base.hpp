#ifndef VOL_BASE_HPP
#define VOL_BASE_HPP

// base class for VOL object

struct VolBase
{
    void init()                     {}
    void term()                     {}

    // info
    void info_copy()                {}
    void info_cmp()                 {}
    void info_free()                {}
    void info_to_str()              {}
    void str_to_info()              {}

    // wrap
    void wrap_get_object()          {}
    void get_wrap_ctx()             {}
    void wrap_object()              {}
    void unwrap_object()            {}
    void free_wrap_ctx()            {}

    // attribute
    void attr_create()              {}
    void attr_open()                {}
    void attr_read()                {}
    void attr_write()               {}
    void attr_get()                 {}
    void attr_specific()            {}
    void attr_optional()            {}
    void atrr_close()               {}

    // dataset
    void dset_create()              {}
    void dset_open()                {}
    void dset_read()                {}
    void dset_write()               {}
    void dset_get()                 {}
    void dset_specific()            {}
    void dset_optional()            {}
    void dset_close()               {}

    // datatype
    void dtype_commit()             {}
    void dtype_open()               {}
    void dtype_get()                {}
    void dtype_specific()           {}
    void dtype_optional()           {}
    void dtype_close()              {}

    // file
    void file_create()              {}
    void file_open()                {}
    void file_get()                 {}
    void file_specific()            {}
    void file_optional()            {}
    void file_close()               {}

    // group
    void group_create()             {}
    void group_open()               {}
    void group_get()                {}
    void group_specific()           {}
    void group_optional()           {}
    void group_close()              {}

    // link
    void link_create()              {}
    void link_copy()                {}
    void link_move()                {}
    void link_get()                 {}
    void link_specific()            {}
    void link_optional()            {}

    // object
    void obj_open()                 {}
    void obj_copy()                 {}
    void obj_get()                  {}
    void obj_specific()             {}
    void obj_optional()             {}

    // request
    void req_wait()                 {}
    void req_notify()               {}
    void req_cancel()               {}
    void req_specific()             {}
    void req_optional()             {}
    void req_free()                 {}
};

#endif
