/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "JwkCache.hpp"
#include "common/log/LogContext.hpp"

#include <optional>
#include <string>

namespace cta::auth {

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
};

TokenValidationResult
ValidateJwt(const std::string& encodedJwt, std::shared_ptr<JwkCache> pubkeyCache, cta::log::LogContext& logContext);
}  // namespace cta::auth
