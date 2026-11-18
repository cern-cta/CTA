/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"

#include <optional>
#include <string_view>

namespace cta::runtime {

/// Supported transports for readiness and liveness endpoints.
enum class HealthTransport {
  TCP,
  UNIX,
};

}  // namespace cta::runtime

/// Parse the health endpoint transport from its TOML name.
template<>
struct cta::common::FromString<cta::runtime::HealthTransport> {
  static std::optional<cta::runtime::HealthTransport> tryFrom(std::string_view text) {
    using enum cta::runtime::HealthTransport;
    if (text == "tcp") {
      return TCP;
    }
    if (text == "unix") {
      return UNIX;
    }
    return std::nullopt;
  }
};
