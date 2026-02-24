/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ConfigMeta.hpp"

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
 * @brief Failure policy for non-critical subsystems in a service.
 */
enum class InitFailurePolicy { warn, fatal };

/**
 * @brief Experimental config that may be common to all apps/tools.
 * Must follow the naming convention: `<feature>_enabled`.
 * Experimental config options relevant only to a specific app/tool MUST NOT be added here.
 * Instead, extend this struct in the relevant app/tool.
 *
 */
struct ExperimentalConfig {
  bool telemetry_enabled = false;

  static consteval auto fields() {
    return std::make_tuple(field("telemetry_enabled", &ExperimentalConfig::telemetry_enabled));
  }
};

/**
 * @brief Catalogue config.
 */
struct CatalogueConfig {
  std::string config_file = "/etc/cta/cta-catalogue.conf";

  static consteval auto fields() { return std::make_tuple(field("config_file", &CatalogueConfig::config_file)); }
};

/**
 * @brief Scheduler config.
 */
struct SchedulerConfig {
  // This value should eventually be handled by auto-discovery and not be provided by users
  std::string backend_name = "";

#ifndef CTA_PGSCHED
  std::string objectstore_backend_path = "";
#else
  std::string config_file = "/etc/cta/cta-scheduler.conf";
#endif
  int tape_cache_max_age_secs = 600;
  int retrieve_queue_cache_max_age_secs = 10;

  static consteval auto fields() {
    return std::make_tuple(
      field("backend_name", &SchedulerConfig::backend_name),
#ifndef CTA_PGSCHED
      field("objectstore_backend_path", &SchedulerConfig::objectstore_backend_path),
#else
      field("config_file", &SchedulerConfig::config_file),
#endif
      field("tape_cache_max_age_secs", &SchedulerConfig::tape_cache_max_age_secs),
      field("retrieve_queue_cache_max_age_secs", &SchedulerConfig::retrieve_queue_cache_max_age_secs));
  }
};

/**
 * @brief Logging config.
 */
struct LoggingConfig {
  std::string level = "INFO";
  std::string format = "json";
  std::map<std::string, std::string> attributes;

  static consteval auto fields() {
    return std::make_tuple(field("level", &LoggingConfig::level),
                           field("format", &LoggingConfig::format),
                           field("attributes", &LoggingConfig::attributes));
  }
};

/**
 * @brief Telemetry config.
 */
struct TelemetryConfig {
  /**
   * @brief Path to the OpenTelemetry declarative config file.
   */
  std::string config_file = "";
  // InitFailurePolicy on_init_failure = InitFailurePolicy::warn;
  std::string on_init_failure = "warn";

  static consteval auto fields() {
    return std::make_tuple(field("config_file", &TelemetryConfig::config_file),
                           field("on_init_failure", &TelemetryConfig::on_init_failure));
  }
};

/**
 * @brief HealthServer config. For applications only.
 */
struct HealthServerConfig {
  bool enabled = false;
  bool use_unix_domain_socket = false;
  std::optional<std::string> host = "";
  std::optional<int> port = 8080;

  static consteval auto fields() {
    return std::make_tuple(field("enabled", &HealthServerConfig::enabled),
                           field("use_unix_domain_socket", &HealthServerConfig::use_unix_domain_socket),
                           field("host", &HealthServerConfig::host),
                           field("port", &HealthServerConfig::port));
  }
};

/**
 * @brief XRootD config to ensure we don't need to rely on environment variables.
 *
 */
struct XRootDConfig {
  std::string security_protocol = "sss";
  std::string sss_keytab_path = "etc/cta/sss.keytab";

  static consteval auto fields() {
    return std::make_tuple(field("security_protocol", &XRootDConfig::security_protocol),
                           field("sss_keytab_path", &XRootDConfig::sss_keytab_path));
  }
};

}  // namespace cta::runtime
