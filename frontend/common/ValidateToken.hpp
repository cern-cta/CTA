#pragma once

#include "common/JwkCache.hpp"
#include "common/log/LogContext.hpp"

namespace cta {
bool ValidateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext logContext);
}