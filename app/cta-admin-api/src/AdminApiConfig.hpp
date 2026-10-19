/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminCmdMode.hpp"
#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/InstanceConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "grpc-server/config/VerificationConfig.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
#include "service-resources/config/FilesConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace cta::admin_api {

/**
 * @brief Repack-related options of the admin API.
 */
struct RepackConfig final {
  std::optional<std::string> buffer_url = std::nullopt;        //!< URL of the repack buffer
  std::optional<uint64_t> max_files_to_select = std::nullopt;  //!< Max number of files to expand during a repack

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (buffer_url.has_value() && buffer_url->empty()) {
      result.addError("buffer_url", "cannot be empty when provided");
    }
    if (max_files_to_select.has_value() && *max_files_to_select == 0) {
      result.addError("max_files_to_select", "must be greater than zero");
    }
    return result;
  }
};

/**
 * @brief Admin-related options of the admin API.
 */
struct AdminConfig final {
  /**
   * Role of the admin API with respect to the repacks, meant to match the scheduler DB it is connected to when the
   * user and the repack requests are served from separate ones (see AdminCmdMode):
   * - `all`: no restriction
   * - `no_repack`: all the repack commands (repack add, rm, err and ls) are rejected, and the tape state changes
   *   follow the rules of a user-only scheduler DB.
   * - `repack`: no command is rejected, and the tape state changes follow the rules of a repack-only scheduler DB.
   */
  AdminCmdMode mode = AdminCmdMode::ALL;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

/**
 * @brief Archive-related options of the admin API.
 */
struct AdminArchiveConfig final {
  uint64_t max_file_size_gb = 0;  //!< Default maximum size of an archived file, in GB (0 = no limit)

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

struct AdminApiConfig final {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::ExperimentalConfig experimental;

  cta::service_resources::CatalogueConfig frontend_catalogue;
  cta::service_resources::FilesConfig files;
#ifndef CTA_PGSCHED
  cta::service_resources::SchedulerConfig frontend_scheduler;
#endif

  cta::grpc_server::ListenerConfig grpc;
  cta::grpc_server::AdminAuthConfig auth;

  cta::grpc_server::InstanceConfig instance;
  AdminConfig admin;
  AdminArchiveConfig archive;

  RepackConfig repack;
  cta::grpc_server::VerificationConfig verification;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 15; }
#else
  static constexpr std::size_t memberCount() { return 14; }
#endif

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("telemetry", telemetry.validate());
    result.merge("experimental", experimental.validate());
    result.merge("frontend_catalogue", frontend_catalogue.validate());
    result.merge("files", files.validate());
#ifndef CTA_PGSCHED
    result.merge("frontend_scheduler", frontend_scheduler.validate());
#endif
    result.merge("instance", instance.validate());
    result.merge("admin", admin.validate());
    result.merge("archive", archive.validate());
    result.merge("grpc", grpc.validate());
    result.merge("auth", auth.validate());
    result.merge("repack", repack.validate());
    result.merge("verification", verification.validate());
    return result;
  }
};

}  // namespace cta::admin_api
