#pragma once

#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_sinks.h>

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

inline
std::shared_ptr<spd::logger>
create_logger(std::string log_level)
{
    auto log = spd::stderr_logger_mt("lowfive");
    int lvl = spd::level::from_str(log_level);
    log->set_level(static_cast<spd::level::level_enum>(lvl));
    return log;
}

template<class... Args>
std::shared_ptr<spd::logger>
set_logger(Args... args)
{
    auto log = std::make_shared<spdlog::logger>("lowfive", args...);
    return log;
}

}
