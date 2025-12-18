/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "version.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>

// need to implement health check for our CI
#include "TokenStorage.hpp"
#include "common/JwkCache.hpp"
#include "common/log/Logger.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "frontend/common/FrontendService.hpp"

#include <grpcpp/health_check_service_interface.h>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

using cta::Scheduler;
using cta::catalogue::Catalogue;
using cta::log::LogContext;
using cta::xrd::CtaRpc;

using grpc::ResourceQuota;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace cta::frontend::grpc {
class CtaRpcImpl : public CtaRpc::Service {
private:
  std::shared_ptr<cta::frontend::FrontendService> m_frontendService;
  ::grpc::HealthCheckServiceInterface* m_healthCheckService = nullptr;
  std::shared_ptr<JwkCache> m_pubkeyCache;
  server::TokenStorage& m_tokenStorage;  // required for the Admin rpc for Kerberos token validation

public:
  CtaRpcImpl(std::shared_ptr<cta::frontend::FrontendService> frontendService,
             std::shared_ptr<JwkCache> pubkeyCache,
             server::TokenStorage& tokenStorage);

  FrontendService& getFrontendService() const { return *m_frontendService; }

  // Archive/Retrieve interface
  Status Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;
  Status Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;
  Status Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;
  Status
  CancelRetrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;
  Status Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;
  // Non-streaming cta-admin commands interface
  Status Admin(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;

private:
  std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
  checkWFERequestAuthMetadata(::grpc::ServerContext* context,
                              const cta::xrd::Request* request,
                              cta::log::LogContext& lc);

  Status processGrpcRequest(const cta::xrd::Request* request,
                            cta::xrd::Response* response,
                            cta::log::LogContext& lc,
                            const cta::common::dataStructures::SecurityIdentity& clientIdentity) const;
};
}  // namespace cta::frontend::grpc
