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
struct ExperimentalConfig final {
  bool telemetry_enabled = false;

  static constexpr std::size_t memberCount() { return 1; }
};

/**
 * @brief Catalogue config.
 */
struct CatalogueConfig final {
  std::string config_file = "/etc/cta/cta-catalogue.conf";

  static constexpr std::size_t memberCount() { return 1; }
};

/**
 * @brief Scheduler config.
 */
struct SchedulerConfig final {
  // This value should eventually be handled by auto-discovery and not be provided by users
  std::string backend_name = "";

#ifndef CTA_PGSCHED
  std::string objectstore_backend_path = "";
#else
  std::string config_file = "/etc/cta/cta-scheduler.conf";
#endif
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;

  static constexpr std::size_t memberCount() { return 4; }
};

/**
 * @brief Logging config.
 */
struct LoggingConfig final {
  std::string level = "INFO";
  std::string format = "json";
  std::map<std::string, std::string> attributes;

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief Telemetry config.
 */
struct TelemetryConfig final {
  /**
   * @brief Path to the OpenTelemetry declarative config file.
   */
  std::string config_file = "";
  std::string on_init_failure = "warn";

  static constexpr std::size_t memberCount() { return 2; }
};

/**
 * @brief HealthServer config. For applications only.
 */
struct HealthServerConfig final {
  bool enabled = false;
  bool use_unix_domain_socket = false;
  std::optional<std::string> host = "";
  std::optional<int> port = 8080;

  static constexpr std::size_t memberCount() { return 4; }
};

/**
 * @brief XRootD config to ensure we don't need to rely on environment variables.
 *
 */
struct XRootDConfig final {
  std::string security_protocol = "sss";
  std::string sss_keytab_path = "etc/cta/sss.keytab";

  static constexpr std::size_t memberCount() { return 2; }
};

}  // namespace cta::runtime
