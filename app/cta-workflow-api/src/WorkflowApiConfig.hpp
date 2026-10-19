/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/CatalogueConfig.hpp"
#include "grpc-server/config/FilesConfig.hpp"
#include "grpc-server/config/InstanceConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "grpc-server/config/SchedulerConfig.hpp"
#include "grpc-server/config/VerificationConfig.hpp"
#include "runtime/config/CommonConfig.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cta::workflow_api {

/**
 * @brief Archive-related options of the workflow API.
 */
struct ArchiveConfig final {
  uint64_t max_file_size_gb = 0;                       //!< Maximum size of an archived file, in GB (0 = no limit)
  bool zero_length_files_forbidden = true;             //!< Whether 0-length files are rejected
  std::vector<std::string> zero_length_vo_exceptions;  //!< VOs exempt from the 0-length file restriction

  static constexpr std::size_t memberCount() { return 3; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

#ifdef CTA_PGSCHED

/**
 * @brief Opportunistic batching of archive/retrieve queueing (PostgreSQL scheduler only).
 *
 * Concurrent queueing requests are coalesced into a single bulk scheduler DB insert. The leader waits for
 * concurrent requests to join its batch until either the window elapses or the batch reaches max_batch_size,
 * whichever comes first.
 */
struct OpportunisticBatchingConfig final {
  bool enabled = false;            //!< Whether concurrent queueing requests are batched
  uint64_t window_ms = 10;         //!< Maximum time (ms) the leader waits for requests to join its batch
  uint64_t max_batch_size = 1000;  //!< Maximum number of requests in a batch

  static constexpr std::size_t memberCount() { return 3; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (enabled && max_batch_size == 0) {
      result.addError("max_batch_size", "must be greater than zero");
    }
    return result;
  }
};

#endif

struct WorkflowApiConfig final {
  cta::runtime::CatalogueConfig catalogue;
  cta::runtime::SchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::ExperimentalConfig experimental;

  cta::grpc_server::CatalogueConfig frontend_catalogue;
  cta::grpc_server::FilesConfig files;
#ifndef CTA_PGSCHED
  cta::grpc_server::SchedulerConfig frontend_scheduler;
#else
  OpportunisticBatchingConfig opportunistic_batching;
#endif

  cta::grpc_server::ListenerConfig grpc;
  cta::grpc_server::WfeAuthConfig auth;

  ArchiveConfig archive;
  cta::grpc_server::VerificationConfig verification;
  cta::grpc_server::InstanceConfig instance;

  static constexpr std::size_t memberCount() { return 13; }

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
#else
    result.merge("opportunistic_batching", opportunistic_batching.validate());
#endif
    result.merge("instance", instance.validate());
    result.merge("verification", verification.validate());
    result.merge("grpc", grpc.validate());
    // Workflow API: exactly one of JWT or mTLS (Kerberos is not representable in WfeAuthConfig)
    result.merge("auth", auth.validate());
    result.merge("archive", archive.validate());
    return result;
  }
};

}  // namespace cta::workflow_api
