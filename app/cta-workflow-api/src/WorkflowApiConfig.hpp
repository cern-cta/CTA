/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
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

/// @brief Prepare (retrieve) request options of the workflow API.
struct PrepareConfig final {
  /// Mount policy used for verification requests
  std::string verification_mount_policy = "";

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

/// @brief Request-related options of the workflow API.
struct RequestsConfig final {
  ArchiveConfig archive;
  PrepareConfig prepare;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("archive", archive.validate());
    result.merge("prepare", prepare.validate());
    return result;
  }
};

#ifdef CTA_PGSCHED

/// @brief Opportunistic batching of archive/retrieve queueing (PostgreSQL scheduler only).
///
/// Concurrent queueing requests are coalesced into a single bulk scheduler DB insert. The leader waits for
/// concurrent requests to join its batch until either the window elapses or the batch reaches max_batch_size,
/// whichever comes first.
struct OpportunisticBatchingConfig final {
  /// Maximum time (ms) the leader waits for requests to join its batch
  uint64_t window_ms = 10;
  /// Maximum number of requests in a batch
  uint64_t max_batch_size = 1000;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (max_batch_size == 0) {
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
  std::optional<cta::runtime::TelemetryConfig> telemetry = std::nullopt;
  std::vector<std::string> enabled_features;

#ifdef CTA_PGSCHED
  std::optional<OpportunisticBatchingConfig> opportunistic_batching = std::nullopt;
#endif

  cta::grpc_server::ListenerConfig grpc;
  cta::grpc_server::WfeAuthConfig auth;

  RequestsConfig requests;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 9; }
#else
  static constexpr std::size_t memberCount() { return 10; }
#endif

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;

    if (instance_name.empty()) {
      result.addError("instance_name", "cannot be empty");
    }

    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    cta::runtime::validateFeature(result, enabled_features, "telemetry", telemetry);
#ifdef CTA_PGSCHED
    result.merge("", cta::runtime::validateFeatureNames(enabled_features, {"telemetry", "opportunistic_batching"}));
#else
    result.merge("", cta::runtime::validateFeatureNames(enabled_features, {"telemetry"}));
#endif
#ifdef CTA_PGSCHED
    cta::runtime::validateFeature(result, enabled_features, "opportunistic_batching", opportunistic_batching);
#endif
    result.merge("requests", requests.validate());
    result.merge("grpc", grpc.validate());
    // Workflow API: exactly one of JWT or mTLS (Kerberos is not representable in WfeAuthConfig)
    result.merge("auth", auth.validate());
    return result;
  }
};

}  // namespace cta::workflow_api
