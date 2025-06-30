
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
class CtaRpcImpl : public CtaRpc::CallbackService {

private:
  std::unique_ptr<cta::frontend::FrontendService> m_frontendService;
  ::grpc::HealthCheckServiceInterface* m_healthCheckService = nullptr;
  bool m_isServing = true;

public:
  CtaRpcImpl(const std::string& config);

  FrontendService& getFrontendService() const { return *m_frontendService; }
  void setHealthCheckService(
    ::grpc::HealthCheckServiceInterface* healthCheckService) {
      m_healthCheckService = healthCheckService;
  }

  // Archive/Retrieve interface
  ::grpc::ServerUnaryReactor* Create(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  ::grpc::ServerUnaryReactor* Archive(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  ::grpc::ServerUnaryReactor* Retrieve(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  ::grpc::ServerUnaryReactor* CancelRetrieve(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
  ::grpc::ServerUnaryReactor* Delete(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response);
private:
  void ProcessGrpcRequest(::grpc::ServerUnaryReactor* reactor, const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext &lc) const;
};
} // namespace cta::frontend::grpc
