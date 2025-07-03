#pragma once

#include "common/JwkCache.hpp"
#include "common/log/LogContext.hpp" // maybe??

namespace cta {
    bool ValidateToken(const std::string& encodedJWT, JwkCache& pubkeyCache, cta::log::LogContext logContext);
}