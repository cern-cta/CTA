/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/runtime/config/CommonConfig.hpp"

#include <string>

namespace cta::rmcd {

struct MediaChangerConfig final {
  std::string device;

  static constexpr std::size_t memberCount() { return 1; }
};

struct ServerConfig final {
  int port;
  std::string listen_scope;

  static constexpr std::size_t memberCount() { return 2; }
};

struct RmcdConfig final {
  cta::runtime::LoggingConfig logging;
  // cta::runtime::TelemetryConfig telemetry; // No telemetry for rmcd (yet?)
  cta::runtime::HealthServerConfig health_server;
  cta::runtime::ExperimentalConfig experimental;
  MediaChangerConfig media_changer;
  ServerConfig server;

  static constexpr std::size_t memberCount() { return 5; }
};

}  // namespace cta::rmcd
