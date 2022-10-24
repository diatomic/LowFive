#pragma once

#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_sinks.h>

namespace spdlog
{
    class logger;
}

namespace LowFive
{

namespace spd = ::spdlog;

inline
std::shared_ptr<spd::logger>
get_logger()
{
    auto log = spd::get("lowfive");
    if (!log)
    {
        auto null_sink = std::make_shared<spd::sinks::null_sink_mt> ();
        log = std::make_shared<spd::logger>("null_logger", null_sink);
    }
    return log;
}

inline
spd::level::level_enum
get_log_level()
{
    auto log = spd::get("lowfive");
    if (log)
        return log->level();
    else
        return spd::level::off;
}

template<class... Args>
std::shared_ptr<spd::logger>
set_logger(Args... args)
{
    auto log = std::make_shared<spdlog::logger>("lowfive", args...);
    return log;
}

template<class... Args>
void
log_assert(bool cond, Args... args)
{
    if (!cond)
    {
        get_logger()->critical(args...);
        std::exit(1);
    }
}

}
