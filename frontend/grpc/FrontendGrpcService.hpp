/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2021 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "version.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>

// need to implement health check for our CI
#include <grpcpp/health_check_service_interface.h>

#include <scheduler/Scheduler.hpp>
#include "common/log/Logger.hpp"
#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "frontend/common/FrontendService.hpp"
#include "common/JwkCache.hpp"
#include "frontend/common/AdminCmd.hpp"

using cta::Scheduler;
using cta::catalogue::Catalogue;
using cta::xrd::CtaRpc;
using cta::log::LogContext;

using grpc::ResourceQuota;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace cta::frontend::grpc {
class CtaRpcImpl : public CtaRpc::Service {

private:
  std::unique_ptr<cta::frontend::FrontendService> m_frontendService;
  ::grpc::HealthCheckServiceInterface* m_healthCheckService = nullptr;
  CurlJwksFetcher m_jwksFetcher;
  std::shared_ptr<JwkCache> m_pubkeyCache;

public:
  CtaRpcImpl(const std::string& config);

  FrontendService& getFrontendService() const { return *m_frontendService; }

  std::shared_ptr<JwkCache> getPubkeyCache() const { return m_pubkeyCache; }
  // Archive/Retrieve interface
  Status Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status CancelRetrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  // Non-streaming cta-admin commands interface
  Status Admin(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);

private:
  Status processGrpcRequest(const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext &lc, const cta::common::dataStructures::SecurityIdentity& clientIdentity) const;
  std::pair<Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
  extractAuthHeaderAndValidate(const ::grpc::ServerContext* context, const cta::xrd::Request* request) const;
};
} // namespace cta::frontend::grpc
