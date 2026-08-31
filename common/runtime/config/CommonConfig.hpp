/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>

/**
 * @brief Common configuration for all application/tools.
 * Depending on what the app/tool requires, the structs below can be referenced.
 * Every value MUST be initialised. Each struct's validate() method enforces semantic
 * constraints which cannot be expressed by the TOML loader's type and bounds checks.
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

  void validate() const {}
};

/**
 * @brief Catalogue config.
 */
struct CatalogueConfig final {
  std::string config_file = "/etc/cta/cta-catalogue.conf";

  static constexpr std::size_t memberCount() { return 1; }

  void validate() const {
    if (config_file.empty()) {
      throw exception::UserError("catalogue.config_file cannot be empty");
    }
  }
};

/**
 * @brief Scheduler config.
 */
struct SchedulerConfig final {
  // This value should eventually be handled by auto-discovery and not be provided by users
  std::string backend_name = "";

  std::string config_file = "/etc/cta/cta-scheduler.conf";

  unsigned int tape_cache_max_age_secs = 600;
  unsigned int retrieve_queue_cache_max_age_secs = 10;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 4; }
#else
  unsigned int number_of_connections = 3;

  static constexpr std::size_t memberCount() { return 5; }
#endif

  void validate() const {
    if (config_file.empty()) {
      throw exception::UserError("scheduler.config_file cannot be empty");
    }
    if (tape_cache_max_age_secs == 0) {
      throw exception::UserError("scheduler.tape_cache_max_age_secs must be positive");
    }
    if (retrieve_queue_cache_max_age_secs == 0) {
      throw exception::UserError("scheduler.retrieve_queue_cache_max_age_secs must be positive");
    }
#ifdef CTA_PGSCHED
    if (number_of_connections == 0) {
      throw exception::UserError("scheduler.number_of_connections must be positive");
    }
#endif
  }
};

/**
 * @brief Logging config.
 */
struct LoggingConfig final {
  std::string level = "INFO";
  std::string format = "json";
  std::map<std::string, std::string> attributes;

  static constexpr std::size_t memberCount() { return 3; }

  void validate() const {
    static constexpr std::array validLevels =
      {"EMERG", "ALERT", "CRIT", "ERR", "WARNING", "NOTICE", "INFO", "DEBUG", "USERERR"};
    static constexpr std::array validFormats = {"kv", "json"};

    if (std::ranges::find(validLevels, level) == validLevels.end()) {
      throw exception::UserError("Unsupported logging.level: '" + level + "'");
    }
    if (std::ranges::find(validFormats, format) == validFormats.end()) {
      throw exception::UserError("Unsupported logging.format: '" + format + "'");
    }
  }
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

  void validate() const {
    if (on_init_failure != "fatal" && on_init_failure != "warn") {
      throw exception::UserError("Unsupported telemetry.on_init_failure: '" + on_init_failure
                                 + "'. Must be one of [fatal, warn].");
    }
  }
};

/**
 * @brief HealthServer config. For applications only.
 */
struct HealthServerConfig final {
  bool enabled = false;
  bool use_unix_domain_socket = false;
  std::optional<std::string> host = "";
  std::optional<unsigned int> port = 8080;

  static constexpr std::size_t memberCount() { return 4; }

  void validate() const {
    if (!enabled) {
      return;
    }
    if (use_unix_domain_socket) {
      return;
    }
    if (!host.has_value()) {
      throw exception::UserError("health_server.host must be provided for TCP");
    }
    if (host->empty()) {
      throw exception::UserError("health_server.host cannot be empty for TCP");
    }
    if (!port.has_value()) {
      throw exception::UserError("health_server.port must be provided for TCP");
    }
    if (*port == 0 || *port > 65535) {
      throw exception::UserError("health_server.port must be between 1 and 65535");
    }
  }
};

/**
 * @brief XRootD config to ensure we don't need to rely on environment variables.
 *
 */
struct XRootDConfig final {
  std::string security_protocol = "sss";
  std::string sss_keytab_path = "etc/cta/sss.keytab";

  static constexpr std::size_t memberCount() { return 2; }

  void validate() const {
    if (security_protocol.empty()) {
      throw exception::UserError("xrootd.security_protocol cannot be empty");
    }
    if (security_protocol == "sss") {
      if (sss_keytab_path.empty()) {
        throw exception::UserError("xrootd.sss_keytab_path cannot be empty when using the sss security protocol");
      }
    }
  }
};

}  // namespace cta::runtime
