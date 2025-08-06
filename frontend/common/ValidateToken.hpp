#pragma once

#include "common/JwkCache.hpp"
#include "common/log/LogContext.hpp"
#include <string>
#include <optional>

namespace cta {

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
};

TokenValidationResult validateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext logContext);

// TokenValidationResult validateTokenAndExtractSub(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext logContext);
}