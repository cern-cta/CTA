/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/AuthMethod.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "runtime/config/CommonConfig.hpp"
#include "service-resources/config/CatalogueConfig.hpp"
#include "service-resources/config/SchedulerConfig.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace cta::workflow_api {

/// Authentication method selection and credentials for workflow API requests.
struct WfeAuthConfig final {
  /// JWT is the non-strict default; strict mode requires an explicit selection.
  std::vector<std::string> enabled_methods = {"jwt"};
  /// JWT auth config (optional)
  std::optional<cta::grpc_server::JwtAuthConfig> jwt;
  /// mTLS auth config (optional)
  std::optional<cta::grpc_server::MtlsAuthConfig> mtls;

  static constexpr auto methods = std::tuple {
    cta::runtime::stableFeature(cta::grpc_server::AuthMethod::JWT, "jwt", &WfeAuthConfig::jwt),
    cta::runtime::stableFeature(cta::grpc_server::AuthMethod::MTLS, "mtls", &WfeAuthConfig::mtls),
  };

  /// Query only authentication methods supported by this service.
  bool methodEnabled(cta::grpc_server::AuthMethod method) const {
    return cta::runtime::featureEnabled(enabled_methods, method, methods);
  }

  /// Validate method selection and the settings of each selected method.
  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatureSelection(*this, enabled_methods, methods, "enabled_methods");
    if (methodEnabled(cta::grpc_server::AuthMethod::JWT) == methodEnabled(cta::grpc_server::AuthMethod::MTLS)) {
      result.addError("enabled_methods",
                      "WFE frontend authentication requires exactly one of JWT or mTLS to be enabled");
    }
    return result;
  }

  static constexpr std::size_t memberCount() { return 3; }
};

/// Archive-related options of the workflow API.
struct ArchiveConfig final {
  /// Whether 0-length files are rejected
  bool zero_length_files_forbidden = true;
  /// VOs exempt from the 0-length file restriction
  std::vector<std::string> zero_length_vo_exceptions;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

/// Prepare (retrieve) request options of the workflow API.
struct PrepareConfig final {
  /// Mount policy used for verification requests
  std::string verification_mount_policy = "";

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

/// Request-related options of the workflow API.
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

/// Backend connections, authentication, request handling and monitoring for the workflow API.
struct WorkflowApiConfig final {
  enum class Feature {
    Telemetry,
#ifdef CTA_PGSCHED
    Batching,
#endif
  };

  /// Name of the instance, used by auth verification and gRPC messages
  std::string instance_name;

  cta::service_resources::ServiceCatalogueConfig catalogue;
  cta::service_resources::ServiceSchedulerConfig scheduler;
  cta::runtime::LoggingConfig logging;
  std::optional<cta::runtime::TelemetryConfig> telemetry = std::nullopt;
  std::vector<std::string> enabled_features;
  std::vector<std::string> enabled_experimental_features;

#ifdef CTA_PGSCHED
  std::optional<OpportunisticBatchingConfig> opportunistic_batching = std::nullopt;
#endif

  cta::grpc_server::ListenerConfig grpc;
  WfeAuthConfig auth;

  RequestsConfig requests;

#ifndef CTA_PGSCHED
  static constexpr std::size_t memberCount() { return 10; }
#else
  static constexpr std::size_t memberCount() { return 11; }
#endif

  static constexpr auto features = std::tuple {
    cta::runtime::experimentalFeature(Feature::Telemetry, "telemetry", &WorkflowApiConfig::telemetry),
#ifdef CTA_PGSCHED
    cta::runtime::experimentalFeature(Feature::Batching,
                                      "opportunistic_batching",
                                      &WorkflowApiConfig::opportunistic_batching),
#endif
  };

  /// Query selection using the feature's stable or experimental list.
  bool featureEnabled(Feature feature) const { return cta::runtime::featureEnabled(*this, feature); }

  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatures(*this);

    if (instance_name.empty()) {
      result.addError("instance_name", "cannot be empty");
    }

    result.merge("catalogue", catalogue.validate());
    result.merge("scheduler", scheduler.validate());
    result.merge("logging", logging.validate());
    result.merge("requests", requests.validate());
    result.merge("grpc", grpc.validate());
    // Workflow API: exactly one of JWT or mTLS (Kerberos is not representable in WfeAuthConfig)
    result.merge("auth", auth.validate());
    return result;
  }
};

}  // namespace cta::workflow_api
