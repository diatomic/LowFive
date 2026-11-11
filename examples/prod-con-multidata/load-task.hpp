#ifndef LOWFIVE_EXAMPLES_LOAD_TASK_HPP
#define LOWFIVE_EXAMPLES_LOAD_TASK_HPP

#include <cstdio>
#include <string>
#include <dlfcn.h>

namespace lowfive_examples
{

// The caller owns library. Keep successful task libraries loaded until process
// exit: HDF5 may retain callbacks into them after their entry points return.
inline void* load_task(const std::string& path, const char* symbol,
                       void*& library, int rank)
{
    dlerror();
    library = dlopen(path.c_str(), RTLD_LAZY);
    if (!library)
    {
        const char* error = dlerror();
        std::fprintf(stderr, "Rank %d: cannot load task library '%s' for entry point '%s': %s\n",
                     rank, path.c_str(), symbol, error ? error : "dlopen returned null");
        return nullptr;
    }

    dlerror();
    void* entry = dlsym(library, symbol);
    const char* error = dlerror();
    if (error || !entry)
    {
        std::fprintf(stderr, "Rank %d: cannot load entry point '%s' from task library '%s': %s\n",
                     rank, symbol, path.c_str(), error ? error : "symbol has a null address");
        return nullptr;
    }
    return entry;
}

} // namespace lowfive_examples

#endif
