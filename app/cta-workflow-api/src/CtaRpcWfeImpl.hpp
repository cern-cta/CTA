/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "WorkflowApiConfig.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"
#include "scheduler/batching/FrontendBatchingLayer.hpp"
#include "service-resources/ServiceResources.hpp"

#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::workflow_api {

/// @brief Implements the archive/retrieve/delete workflow RPCs (EOS Workflow Events)
///
/// The Admin RPCs are not served by the workflow API, so they keep the default UNIMPLEMENTED behaviour set
/// by the auto-generated code
class CtaRpcWfeImpl final : public cta::xrd::CtaRpc::Service {
public:
  CtaRpcWfeImpl(service_resources::ServiceResources& serviceResources,
                const WorkflowApiConfig& config,
#ifdef CTA_PGSCHED
                FrontendBatchingLayer& batchingLayer,
#endif
                std::shared_ptr<grpc_server::auth::JwtAuthManager> jwtAuthManager);

  ::grpc::Status
  Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) override;
  ::grpc::Status
  Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) override;
  ::grpc::Status
  Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) override;
  ::grpc::Status CancelRetrieve(::grpc::ServerContext* context,
                                const cta::xrd::Request* request,
                                cta::xrd::Response* response) override;
  ::grpc::Status
  Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) override;

private:
  /// @brief Extra per-RPC checks, run after the event type check. Returns a non-OK status to reject the request.
  /// May also add request-specific params to the log container.
  using RequestCheck = std::function<::grpc::Status(log::LogContext& lc, log::ScopedParamContainer& sp)>;

  /// @brief RPC workflow:
  ///  - authentication
  ///  - event type check
  ///  - extra checks (`check` parameter)
  ///  - processing (handled by `processGrpcRequest`)
  ::grpc::Status handleWorkflowEvent(::grpc::ServerContext* context,
                                     const cta::xrd::Request* request,
                                     cta::xrd::Response* response,
                                     const char* requestName,
                                     cta::eos::Workflow_EventType expectedEvent,
                                     const RequestCheck& check) const;

  /// @brief True if the clients authenticate with an mTLS certificate, false if they use a JWT
  bool usesMtls() const;

  /// @brief Certificate identities (SAN) which are accepted for the given EOS instance
  std::set<std::string, std::less<>> getMtlsCertIdentitiesForInstance(const std::string& instance) const;

  /// @brief Authenticates the caller with the configured method (mTLS or JWT)
  ///
  /// @return OK with the client identity, or an UNAUTHENTICATED status without identity
  std::pair<::grpc::Status, std::optional<common::dataStructures::SecurityIdentity>>
  validateRequestAuthMetadata(::grpc::ServerContext* context,
                              const cta::xrd::Request* request,
                              log::LogContext& lc) const;

  /// @brief Runs the workflow event and maps the exceptions it throws to the response type and gRPC status
  ///
  /// @param lc              Log context, already carrying the request params
  /// @param clientIdentity  Identity of the authenticated client
  ::grpc::Status processGrpcRequest(const cta::xrd::Request* request,
                                    cta::xrd::Response* response,
                                    log::LogContext& lc,
                                    const common::dataStructures::SecurityIdentity& clientIdentity) const;

  service_resources::ServiceResources& m_serviceResources;
  const WorkflowApiConfig& m_config;
#ifdef CTA_PGSCHED
  /// Opportunistic-batching layer for archive/retrieve queueing
  FrontendBatchingLayer& m_batchingLayer;
#endif
  /// Shared JWK cache for token validation
  std::shared_ptr<grpc_server::auth::JwtAuthManager> m_jwtAuthManager;
};

}  // namespace cta::workflow_api
