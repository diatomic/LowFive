#ifndef EXAMPLE1_VOL_HPP
#define EXAMPLE1_VOL_HPP

#include    <diy/log.hpp>
#include    "../../src/vol.hpp"

// custom VOL object
// only need to specialize those functions that are custom

struct Vol : public VolBase
{
    void init()                     { fmt::print(stderr, "Hello Vol\n"); }
    void term()                     { fmt::print(stderr, "Goodbye Vol\n"); }

    struct Info
    {
        void copy()                 { fmt::print(stderr, "Copy Info\n"); }
    };
};

// ------------------
// All the vol functions need to have C-style wrappers, including those not specialized above
// TODO:
// I'd rather not have this in the user's code, but I don't know how to separate w/o circular dependency
//
// Don't change the function and method names below (fragile!)
//
// C functions                      C++ methods
void* vol_new()                     { return new Vol();     }
void vol_delete(void *vol)          { delete (Vol*)vol;     }
void vol_init(void *vol)            { ((Vol*)vol)->init();  }
void vol_term(void *vol)            { ((Vol*)vol)->term();  }

void info_copy(void* vol)           { ((Vol*)vol)->info.copy(); }
void info_cmp(void* vol)            { ((Vol*)vol)->info.cmp(); }
void info_free(void* vol)           { ((Vol*)vol)->info.free(); }
void info_to_str(void* vol)         { ((Vol*)vol)->info.info_to_str(); }
void info_str_to_info(void* vol)    { ((Vol*)vol)->info.str_to_info(); }

void wrap_get_object(void* vol)     { ((Vol*)vol)->wrap.get_object(); }
void wrap_get_wrap_ctx(void* vol)   { ((Vol*)vol)->wrap.get_wrap_ctx(); }
void wrap_wrap_object(void* vol)    { ((Vol*)vol)->wrap.wrap_object(); }
void wrap_unwrap_object(void* vol)  { ((Vol*)vol)->wrap.unwrap_object(); }
void wrap_free_wrap_ctx(void* vol)  { ((Vol*)vol)->wrap.free_wrap_ctx(); }

void attribute_create(void* vol)    { ((Vol*)vol)->attribute.create(); }
void attribute_open(void* vol)      { ((Vol*)vol)->attribute.open(); }
void attribute_read(void* vol)      { ((Vol*)vol)->attribute.read(); }
void attribute_write(void* vol)     { ((Vol*)vol)->attribute.write(); }
void attribute_get(void* vol)       { ((Vol*)vol)->attribute.get(); }
void attribute_specific(void* vol)  { ((Vol*)vol)->attribute.specific(); }
void attribute_optional(void* vol)  { ((Vol*)vol)->attribute.optional(); }
void attribute_close(void* vol)     { ((Vol*)vol)->attribute.close(); }

void dataset_create(void* vol)      { ((Vol*)vol)->dataset.create(); }
void dataset_open(void* vol)        { ((Vol*)vol)->dataset.open(); }
void dataset_read(void* vol)        { ((Vol*)vol)->dataset.read(); }
void dataset_write(void* vol)       { ((Vol*)vol)->dataset.write(); }
void dataset_get(void* vol)         { ((Vol*)vol)->dataset.get(); }
void dataset_specific(void* vol)    { ((Vol*)vol)->dataset.specific(); }
void dataset_optional(void* vol)    { ((Vol*)vol)->dataset.optional(); }
void dataset_close(void* vol)       { ((Vol*)vol)->dataset.close(); }

void datatype_commit(void* vol)     { ((Vol*)vol)->datatype.commit(); }
void datatype_open(void* vol)       { ((Vol*)vol)->datatype.open(); }
void datatype_get(void* vol)        { ((Vol*)vol)->datatype.get(); }
void datatype_specific(void* vol)   { ((Vol*)vol)->datatype.specific(); }
void datatype_optional(void* vol)   { ((Vol*)vol)->datatype.optional(); }
void datatype_close(void* vol)      { ((Vol*)vol)->datatype.close(); }

void file_create(void* vol)         { ((Vol*)vol)->file.create(); }
void file_open(void* vol)           { ((Vol*)vol)->file.open(); }
void file_get(void* vol)            { ((Vol*)vol)->file.get(); }
void file_specific(void* vol)       { ((Vol*)vol)->file.specific(); }
void file_optional(void* vol)       { ((Vol*)vol)->file.optional(); }
void file_close(void* vol)          { ((Vol*)vol)->file.close(); }

void group_create(void* vol)        { ((Vol*)vol)->group.create(); }
void group_open(void* vol)          { ((Vol*)vol)->group.open(); }
void group_get(void* vol)           { ((Vol*)vol)->group.get(); }
void group_specific(void* vol)      { ((Vol*)vol)->group.specific(); }
void group_optional(void* vol)      { ((Vol*)vol)->group.optional(); }
void group_close(void* vol)         { ((Vol*)vol)->group.close(); }

void link_create(void* vol)         { ((Vol*)vol)->link.create(); }
void link_copy(void* vol)           { ((Vol*)vol)->link.copy(); }
void link_move(void* vol)           { ((Vol*)vol)->link.move(); }
void link_get(void* vol)            { ((Vol*)vol)->link.get(); }
void link_specific(void* vol)       { ((Vol*)vol)->link.specific(); }
void link_optional(void* vol)       { ((Vol*)vol)->link.optional(); }

void object_open(void* vol)         { ((Vol*)vol)->object.open(); }
void object_copy(void* vol)         { ((Vol*)vol)->object.copy(); }
void object_get(void* vol)          { ((Vol*)vol)->object.get(); }
void object_specific(void* vol)     { ((Vol*)vol)->object.specific(); }
void object_optional(void* vol)     { ((Vol*)vol)->object.optional(); }

void request_wait(void* vol)        { ((Vol*)vol)->request.wait(); }
void request_notify(void* vol)      { ((Vol*)vol)->request.notify(); }
void request_cancel(void* vol)      { ((Vol*)vol)->request.cancel(); }
void request_specific(void* vol)    { ((Vol*)vol)->request.specific(); }
void request_optional(void* vol)    { ((Vol*)vol)->request.optional(); }
void request_free(void* vol)        { ((Vol*)vol)->request.free(); }

#endif
