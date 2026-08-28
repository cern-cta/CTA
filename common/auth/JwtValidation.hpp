/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "JwtCache.hpp"
#include "common/log/LogContext.hpp"

#include <optional>
#include <string>

namespace cta::auth {

struct TokenValidationResult {
  bool isValid;
  std::optional<std::string> subjectClaim;
  std::optional<std::string> errorMessage;
};

TokenValidationResult
ValidateJwt(const std::string& encodedJwt, JwtCache& pubkeyCache, const cta::log::LogContext& logContext);
}  // namespace cta::auth
