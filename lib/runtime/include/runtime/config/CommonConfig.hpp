/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "FeatureConfig.hpp"
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

/// @brief Marker for a config section without a `client` sub-table.
struct NoClientConfig final {
  static constexpr std::size_t memberCount() { return 0; }

  ValidationResult validate() const { return ValidationResult::OK; }
};

/// @brief Catalogue config.
/// @tparam ClientConfig Application-specific options of the catalogue client, read from `[catalogue.client]`.
template<typename ClientConfig = NoClientConfig>
struct BasicCatalogueConfig final {
  std::string config_file = "/etc/cta/cta-catalogue.conf";

  ClientConfig client;

  static constexpr std::size_t memberCount() { return 2; }

  ValidationResult validate() const {
    ValidationResult result;
    if (config_file.empty()) {
      result.addError("config_file", "cannot be empty");
    }
    result.merge("client", client.validate());
    return result;
  }
};

/// By default, no client config required
using CatalogueConfig = BasicCatalogueConfig<>;

/// @brief Scheduler config.
/// @tparam ClientConfig Application-specific options of the scheduler client, read from `[scheduler.client]`.
template<typename ClientConfig = NoClientConfig>
struct BasicSchedulerConfig final {
  /// This value should eventually be handled by auto-discovery and not be provided by users
  std::string backend_name = "";

  std::string config_file = "/etc/cta/cta-scheduler.conf";

  uint32_t tape_cache_max_age_secs = 600;
  uint32_t retrieve_queue_cache_max_age_secs = 10;

  ClientConfig client;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 5; }
#else
  uint32_t number_of_connections = 3;

  static constexpr std::size_t memberCount() { return 6; }
#endif

  ValidationResult validate() const {
    ValidationResult result;
    if (backend_name.empty()) {
      result.addError("backend_name", "cannot be empty");
    }
    if (config_file.empty()) {
      result.addError("config_file", "cannot be empty");
    }
    if (tape_cache_max_age_secs == 0) {
      result.addError("tape_cache_max_age_secs", "must be greater than zero");
    }
    if (retrieve_queue_cache_max_age_secs == 0) {
      result.addError("retrieve_queue_cache_max_age_secs", "must be greater than zero");
    }
#ifdef CTA_PGSCHED
    if (number_of_connections == 0) {
      result.addError("number_of_connections", "must be greater than zero");
    }
#endif
    result.merge("client", client.validate());
    return result;
  }
};

/// By default, no client config required
using SchedulerConfig = BasicSchedulerConfig<>;

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
    if (config_file.empty()) {
      result.addError("config_file", "cannot be empty");
    }
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
struct HealthTcpConfig final {
  std::string host = "127.0.0.1";
  uint16_t port = 8080;

  static constexpr std::size_t memberCount() { return 2; }

  ValidationResult validate() const {
    ValidationResult result;
    if (host.empty()) {
      result.addError("host", "cannot be empty");
    }
    if (port == 0) {
      result.addError("port", "must be greater than zero");
    }
    return result;
  }
};

struct HealthServerConfig final {
  std::string transport = "tcp";
  std::optional<HealthTcpConfig> tcp = std::nullopt;

  static constexpr std::size_t memberCount() { return 2; }

  ValidationResult validate() const {
    ValidationResult result;
    if (transport != "tcp" && transport != "unix") {
      result.addError("transport", "must be one of [tcp, unix]");
    }
    validateFeature(result,
                    transport == "tcp" ? std::vector<std::string> {"tcp"} : std::vector<std::string> {},
                    "tcp",
                    tcp);
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
