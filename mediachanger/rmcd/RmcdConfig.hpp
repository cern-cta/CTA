/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/CommonConfig.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace cta::rmcd {

/// Device used to control the tape library through SCSI media-changer commands.
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

/// Listening port and network scope of the remote media-changer protocol server.
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

/// Device, listener and monitoring settings for the remote media-changer daemon.
struct RmcdConfig final {
  cta::runtime::LoggingConfig logging;
  std::optional<cta::runtime::TelemetryConfig> telemetry = std::nullopt;
  std::optional<cta::runtime::HealthServerConfig> health_server = std::nullopt;
  std::vector<std::string> enabled_features;
  std::vector<std::string> enabled_experimental_features;
  MediaChangerConfig media_changer;
  RmcServerConfig rmc_server;

  static constexpr std::size_t memberCount() { return 7; }

  static constexpr auto features =
    std::tuple {cta::runtime::experimentalFeature("telemetry", &RmcdConfig::telemetry),
                cta::runtime::stableFeature("health_server", &RmcdConfig::health_server)};

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("logging", logging.validate());
    result.merge("", cta::runtime::validateFeatures(*this));
    result.merge("media_changer", media_changer.validate());
    result.merge("rmc_server", rmc_server.validate());
    return result;
  }
};

}  // namespace cta::rmcd
