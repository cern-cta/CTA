/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminCmdMode.hpp"
#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
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
 * @brief Options related to how the admin API handles tape file copies and the recycle log.
 */
struct FilesConfig final {
  uint64_t missing_file_copies_min_age_secs = 0;  //!< Minimum age of missing tape file copies before they are reported
  uint64_t recycle_log_quarantine_secs =
    31 * 24 * 60 * 60;  //! Minimum quarantine period before a tape can be reclaimed (default: 31 days).

  static constexpr std::size_t memberCount() { return 2; }

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
  std::string instance_name;  //!< Name of the instance, used by auth verification and gRPC messages

  /**
   * Role of the admin API with respect to the repacks, meant to match the scheduler DB it is connected to when the
   * user and the repack requests are served from separate ones (see AdminCmdMode):
   * - `all`: no restriction
   * - `no_repack`: all the repack commands (repack add, rm, err and ls) are rejected, and the tape state changes
   *   follow the rules of a user-only scheduler DB.
   * - `repack`: no command is rejected, and the tape state changes follow the rules of a repack-only scheduler DB.
   */
  AdminCmdMode admin_mode = AdminCmdMode::ALL;

  cta::service_resources::ServiceCatalogueConfig catalogue;
  cta::service_resources::ServiceSchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::ExperimentalConfig experimental;

  FilesConfig files;

  cta::grpc_server::ListenerConfig grpc;
  cta::grpc_server::AdminAuthConfig auth;
  AdminArchiveConfig archive;

  RepackConfig repack;

  static constexpr std::size_t memberCount() { return 12; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;

    if (instance_name.empty()) {
      result.addError("instance_name", "cannot be empty");
    }

    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("telemetry", telemetry.validate());
    result.merge("experimental", experimental.validate());
    result.merge("files", files.validate());
    result.merge("archive", archive.validate());
    result.merge("grpc", grpc.validate());
    result.merge("auth", auth.validate());
    result.merge("repack", repack.validate());
    return result;
  }
};

}  // namespace cta::admin_api
