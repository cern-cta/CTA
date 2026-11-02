/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/ServerBuilderHelper.hpp"

#include "common/exception/Exception.hpp"
#include "grpc-server/server/TlsCredentials.hpp"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/resource_quota.h>
#include <thread>

namespace cta::grpc_server {

ServerBuilderHelper::ServerBuilderHelper(const ListenerConfig& config,
                                         bool requireClientCertificate,
                                         std::shared_ptr<::grpc::AuthMetadataProcessor> authMetadataProcessor,
                                         log::LogContext& lc)
    : m_serverAddress("0.0.0.0:" + std::to_string(config.port)) {
  lc.log(log::INFO, "Using gRPC over TLS");
  auto creds = createTlsServerCredentials(config.tls, requireClientCertificate, lc);
  if (authMetadataProcessor) {
    creds->SetAuthMetadataProcessor(authMetadataProcessor);
  }

  // enable health checking
  ::grpc::EnableDefaultHealthCheckService(true);
  // add reflection
  ::grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  // Listen on the given address
  m_builder.AddListeningPort(m_serverAddress, creds);

  // Fix the number of request processing threads
  const int threads = config.number_of_threads.value_or(8 * std::thread::hardware_concurrency());
  ::grpc::ResourceQuota quota;
  quota.SetMaxThreads(threads);
  lc.log(log::INFO, "Using " + std::to_string(threads) + " request processing threads");
  m_builder.SetResourceQuota(quota);
}

std::unique_ptr<::grpc::Server> ServerBuilderHelper::buildAndStart() {
  auto server = m_builder.BuildAndStart();
  if (!server) {
    throw exception::Exception("Failed to start gRPC server on " + m_serverAddress
                               + ": check that the port is free and the TLS configuration is valid");
  }
  return server;
}

}  // namespace cta::grpc_server
