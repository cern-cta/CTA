#pragma once

#include "common/JwkCache.hpp"
#include "common/log/LogContext.hpp"

namespace cta {
bool validateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext logContext);
}