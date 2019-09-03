#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol_base.hpp"

// custom VOL object
// only need to specialize those functions that are custom

struct Vol : public VolBase
{
    void init()                     { fmt::print(stderr, "Hello Vol\n"); }
    void term()                     { fmt::print(stderr, "Goodbye Vol\n"); }

    void info_copy()                { fmt::print(stderr, "Copy Info\n"); }
};

#endif
