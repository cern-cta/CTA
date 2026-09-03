/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <string>

namespace cta::rmcd {

struct MediaChangerConfig final {
  std::string device = "/dev/smc";

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (device.empty()) {
      result.addError("device", "cannot be empty");
    }
    return result;
  }
};

struct RmcServerConfig final {
  uint16_t port = 5014;
  std::string listen_scope = "loopback";

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (port == 0) {
      result.addError("port", "must be greater than zero");
    }
    if (listen_scope != "loopback" && listen_scope != "any") {
      result.addError("listen_scope", "has unsupported value '" + listen_scope + "'; must be one of [loopback, any]");
    }
    return result;
  }
};

struct RmcdConfig final {
  cta::runtime::LoggingConfig logging;
  // cta::runtime::TelemetryConfig telemetry; // No telemetry for rmcd (yet?)
  cta::runtime::HealthServerConfig health_server;
  MediaChangerConfig media_changer;
  RmcServerConfig rmc_server;

  static constexpr std::size_t memberCount() { return 4; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("logging", logging.validate());
    result.merge("health_server", health_server.validate());
    result.merge("media_changer", media_changer.validate());
    result.merge("rmc_server", rmc_server.validate());
    return result;
  }
};

}  // namespace cta::rmcd
