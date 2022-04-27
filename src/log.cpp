#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <lowfive/log.hpp>

namespace spdlog
{
    class logger;
}

namespace LowFive
{

namespace spd = ::spdlog;

std::shared_ptr<spd::logger>
create_logger(std::string log_level)
{
    auto log = spd::stderr_logger_mt("lowfive");
    int lvl = spd::level::from_str(log_level);
    log->set_level(static_cast<spd::level::level_enum>(lvl));
    return log;
}


}
