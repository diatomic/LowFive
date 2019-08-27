#ifndef VOL_HPP
#define VOL_HPP

// base class for VOL object

struct VolBase
{
    void init()                     {}
    void term()                     {}

    struct Info
    {
        void copy()                 {}
        void cmp()                  {}
        void free()                 {}
        void info_to_str()          {}
        void str_to_info()          {}
    };

    struct Wrap
    {
        void get_object()           {}
        void get_wrap_ctx()         {}
        void wrap_object()          {}
        void unwrap_object()        {}
        void free_wrap_ctx()        {}
    };

    struct Attribute
    {
        void create()               {}
        void open()                 {}
        void read()                 {}
        void write()                {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
        void close()                {}
    };

    struct Dataset
    {
        void create()               {}
        void open()                 {}
        void read()                 {}
        void write()                {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
        void close()                {}
    };

    struct Datatype
    {
        void commit()               {}
        void open()                 {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
        void close()                {}
    };

    struct File
    {
        void create()               {}
        void open()                 {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
        void close()                {}
    };

    struct Group
    {
        void create()               {}
        void open()                 {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
        void close()                {}
    };

    struct Link
    {
        void create()               {}
        void copy()                 {}
        void move()                 {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
    };

    struct Object
    {
        void open()                 {}
        void copy()                 {}
        void get()                  {}
        void specific()             {}
        void optional()             {}
    };

    struct Request
    {
        void wait()                 {}
        void notify()               {}
        void cancel()               {}
        void specific()             {}
        void optional()             {}
        void free()                 {}
    };

    Info        info;
    Wrap        wrap;
    Attribute   attribute;
    Dataset     dataset;
    Datatype    datatype;
    File        file;
    Group       group;
    Link        link;
    Object      object;
    Request     request;
};

#endif
