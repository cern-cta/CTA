#pragma once

#include "common/JwkCache.hpp"
#include "common/log/LogContext.hpp"

#include <optional>
#include <string>

namespace cta {

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
};

TokenValidationResult
validateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext& logContext);
}  // namespace cta