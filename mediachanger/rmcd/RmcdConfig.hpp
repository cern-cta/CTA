/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/runtime/config/CommonConfig.hpp"

#include <string>

namespace cta::rmcd {

struct MediaChangerConfig final {
  std::string device = "/dev/smc";

  static constexpr std::size_t memberCount() { return 1; }
};

struct RmcServerConfig final {
  int port = 5014;
  std::string listen_scope = "loopback";

  static constexpr std::size_t memberCount() { return 2; }
};

struct RmcdConfig final {
  cta::runtime::LoggingConfig logging;
  // cta::runtime::TelemetryConfig telemetry; // No telemetry for rmcd (yet?)
  cta::runtime::HealthServerConfig health_server;
  MediaChangerConfig media_changer;
  RmcServerConfig rmc_server;

  static constexpr std::size_t memberCount() { return 4; }
};

}  // namespace cta::rmcd
