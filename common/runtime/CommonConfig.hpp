/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace cta::runtime {

struct ExperimentalConfig {
  bool telemetry_enabled = false;
};

struct CatalogueConfig {
  std::string config_file = "/etc/cta/cta-catalogue.conf";
};

struct SchedulerConfig {
  std::string backend_name;
  std::string objectstore_backend_path;
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;
};

struct LoggingConfig {
  std::string level = "INFO";
  std::string format = "json";
  std::map<std::string, std::string> attributes;
};

struct TelemetryConfig {
  std::string config = "";
};

struct HealthServerConfig {
  bool enabled;
  std::string host;
  int port;
};

struct XRootDConfig {
  std::string security_protocol = "sss";
  std::string sss_keytab_path;
};

}  // namespace cta::runtime
