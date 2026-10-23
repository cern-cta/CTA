/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "grpc-server/config/VerificationConfig.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cta::workflow_api {

/// @brief Archive-related options of the workflow API.
struct ArchiveConfig final {
  /// Whether 0-length files are rejected
  bool zero_length_files_forbidden = true;
  /// VOs exempt from the 0-length file restriction
  std::vector<std::string> zero_length_vo_exceptions;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

#ifdef CTA_PGSCHED

/// @brief Opportunistic batching of archive/retrieve queueing (PostgreSQL scheduler only).
///
/// Concurrent queueing requests are coalesced into a single bulk scheduler DB insert. The leader waits for
/// concurrent requests to join its batch until either the window elapses or the batch reaches max_batch_size,
/// whichever comes first.
struct OpportunisticBatchingConfig final {
  /// Whether concurrent queueing requests are batched
  bool enabled = false;
  /// Maximum time (ms) the leader waits for requests to join its batch
  uint64_t window_ms = 10;
  /// Maximum number of requests in a batch
  uint64_t max_batch_size = 1000;

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
  /// Name of the instance, used by auth verification and gRPC messages
  std::string instance_name;

  cta::service_resources::ServiceCatalogueConfig catalogue;
  cta::service_resources::ServiceSchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::ExperimentalConfig experimental;

#ifdef CTA_PGSCHED
  OpportunisticBatchingConfig opportunistic_batching;
#endif

  cta::grpc_server::ListenerConfig grpc;
  cta::grpc_server::WfeAuthConfig auth;

  ArchiveConfig archive;
  cta::grpc_server::VerificationConfig verification;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 10; }
#else
  static constexpr std::size_t memberCount() { return 11; }
#endif

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
#ifdef CTA_PGSCHED
    result.merge("opportunistic_batching", opportunistic_batching.validate());
#endif
    result.merge("verification", verification.validate());
    result.merge("grpc", grpc.validate());
    // Workflow API: exactly one of JWT or mTLS (Kerberos is not representable in WfeAuthConfig)
    result.merge("auth", auth.validate());
    result.merge("archive", archive.validate());
    return result;
  }
};

}  // namespace cta::workflow_api
