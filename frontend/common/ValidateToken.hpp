/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
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

TokenValidationResult
validateToken(const std::string& encodedJWT, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext& logContext);
}
