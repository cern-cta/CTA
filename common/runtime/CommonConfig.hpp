/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

/**
 * @brief Common configuration for all application/tools.
 * Depending on what the app/tool requires, the structs below can be referenced.
 * Every value MUST be initialised. String values MAY be empty, but it is RECOMMENDED
 * to initialise them. Assume that any string MAY be empty, which MUST be checked
 * before it is consumed in the program. This is not checked during config loading.
 *
 * Assume that all of the structs below are referenced by all applications. As such,
 * don't add options specific to an application here, because it will enforce all
 * applications that use that struct to support said option.
 */
namespace cta::runtime {

/**
 * @brief Experimental config that may be common to all apps/tools.
 * Must follow the naming convention: `<feature>_enabled`.
 * Experimental config options relevant only to a specific app/tool MUST NOT be added here.
 * Instead, extend this struct in the relevant app/tool.
 *
 */
struct ExperimentalConfig {
  bool telemetry_enabled = false;
};

/**
 * @brief Catalogue config.
 */
struct CatalogueConfig {
  std::string config_file = "/etc/cta/cta-catalogue.conf";
};

/**
 * @brief Scheduler config.
 */
struct SchedulerConfig {
  // This value should eventually be handled by auto-discovery and not be provided by users
  std::string backend_name = "";
  std::string objectstore_backend_path = "";
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;
};

/**
 * @brief Logging config.
 */
struct LoggingConfig {
  std::string level = "INFO";
  std::string format = "json";
  std::map<std::string, std::string> attributes;
};

/**
 * @brief Telemetry config.
 */
struct TelemetryConfig {
  /**
   * @brief Path to the OpenTelemetry declarative config file.
   */
  std::string config_file = "";
};

/**
 * @brief HealthServer config. For applications only.
 */
struct HealthServerConfig {
  bool enabled = false;
  std::string host = "";
  int port = 8080;
};

/**
 * @brief XRootD config to ensure we don't need to rely on environment variables.
 *
 */
struct XRootDConfig {
  std::string security_protocol = "sss";
  std::string sss_keytab_path = "etc/cta/sss.keytab";
};

}  // namespace cta::runtime
