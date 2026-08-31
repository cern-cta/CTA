/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ValidationResult.hpp"

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

  ValidationResult validate() const { return {}; }
};

/**
 * @brief Catalogue config.
 */
struct CatalogueConfig final {
  std::string config_file = "/etc/cta/cta-catalogue.conf";

  static constexpr std::size_t memberCount() { return 1; }

  ValidationResult validate() const {
    ValidationResult result;
    if (config_file.empty()) {
      result.addError("config_file", "cannot be empty");
    }
    return result;
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

  ValidationResult validate() const {
    ValidationResult result;
    if (config_file.empty()) {
      result.addError("config_file", "cannot be empty");
    }
    if (tape_cache_max_age_secs == 0) {
      result.addError("tape_cache_max_age_secs", "must be positive");
    }
    if (retrieve_queue_cache_max_age_secs == 0) {
      result.addError("retrieve_queue_cache_max_age_secs", "must be positive");
    }
#ifdef CTA_PGSCHED
    if (number_of_connections == 0) {
      result.addError("number_of_connections", "must be positive");
    }
#endif
    return result;
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

  ValidationResult validate() const {
    static constexpr std::array validLevels =
      {"EMERG", "ALERT", "CRIT", "ERR", "WARNING", "NOTICE", "INFO", "DEBUG", "USERERR"};
    static constexpr std::array validFormats = {"kv", "json"};

    ValidationResult result;
    if (std::ranges::find(validLevels, level) == validLevels.end()) {
      result.addError("level", "has unsupported value '" + level + "'");
    }
    if (std::ranges::find(validFormats, format) == validFormats.end()) {
      result.addError("format", "has unsupported value '" + format + "'");
    }
    return result;
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

  ValidationResult validate() const {
    ValidationResult result;
    if (on_init_failure != "fatal" && on_init_failure != "warn") {
      result.addError("on_init_failure",
                      "has unsupported value '" + on_init_failure + "'; must be one of [fatal, warn]");
    }
    return result;
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

  ValidationResult validate() const {
    ValidationResult result;
    if (!enabled) {
      return result;
    }
    if (use_unix_domain_socket) {
      return result;
    }
    if (!host.has_value()) {
      result.addError("host", "must be provided for TCP");
    } else if (host->empty()) {
      result.addError("host", "cannot be empty for TCP");
    }
    if (!port.has_value()) {
      result.addError("port", "must be provided for TCP");
    } else if (*port == 0 || *port > 65535) {
      result.addError("port", "must be between 1 and 65535");
    }
    return result;
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

  ValidationResult validate() const {
    ValidationResult result;
    if (security_protocol.empty()) {
      result.addError("security_protocol", "cannot be empty");
    }
    if (security_protocol == "sss") {
      if (sss_keytab_path.empty()) {
        result.addError("sss_keytab_path", "cannot be empty when using the sss security protocol");
      }
    }
    return result;
  }
};

}  // namespace cta::runtime
