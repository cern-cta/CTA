
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
  bool m_isServing = true;
  JwkCache m_pubkeyCache;
  std::mutex m_cacheMutex;  // to guard cache in multithreaded environment

public:
  CtaRpcImpl(const std::string& config);

  FrontendService& getFrontendService() const { return *m_frontendService; }
  void setHealthCheckService(
    ::grpc::HealthCheckServiceInterface* healthCheckService) {
      m_healthCheckService = healthCheckService;
  }

  void StartJwksRefreshThread() { return m_pubkeyCache.StartRefreshThread(); }
  void StopJwksRefreshThread() { return m_pubkeyCache.StopRefreshThread(); }
  // Archive/Retrieve interface
  Status Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status CancelRetrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  Status Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
private:
  Status ProcessGrpcRequest(const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext &lc) const;
  bool ValidateToken(const std::string& encodedJWT);
  Status ExtractAuthHeaderAndValidate(::grpc::ServerContext* context);
};
} // namespace cta::frontend::grpc
