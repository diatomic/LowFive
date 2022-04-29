#pragma once

#include <string>
#include <memory>

namespace spdlog
{
    class logger;
}

namespace LowFive
{

namespace spd = ::spdlog;

std::shared_ptr<spd::logger>
create_logger(std::string log_level);

}
