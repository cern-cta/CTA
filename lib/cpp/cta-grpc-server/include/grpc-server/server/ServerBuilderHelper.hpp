/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "grpc-server/config/ListenerConfig.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace cta::grpc_server {

/// @brief Prepares a gRPC ServerBuilder with the settings common to the CTA gRPC servers: listening address, TLS,
/// thread quota, health checking and reflection. Services and completion queues are added by the caller through
/// builder(), which is then started with buildAndStart().
class ServerBuilderHelper final {
public:
  /// @param config                    Listener configuration (port, threads, TLS)
  /// @param requireClientCertificate  True to request and verify client certificates (mTLS)
  /// @param authMetadataProcessor     Optional processor run on every request (may be null)
  /// @param lc                        Log context used to report the configuration
  ServerBuilderHelper(const ListenerConfig& config,
                      bool requireClientCertificate,
                      std::shared_ptr<::grpc::AuthMetadataProcessor> authMetadataProcessor,
                      log::LogContext& lc);

  ServerBuilderHelper(const ServerBuilderHelper&) = delete;
  ServerBuilderHelper& operator=(const ServerBuilderHelper&) = delete;

  /// @brief The builder, to register services and completion queues before buildAndStart()
  ::grpc::ServerBuilder& builder() { return m_builder; }

  /// @brief The "0.0.0.0:<port>" address the server listens on
  const std::string& serverAddress() const { return m_serverAddress; }

  /// @brief Builds and starts the server
  /// @return A unique pointer to the running server
  /// @throws cta::exception::Exception if the server could not be started (e.g. the port could not be bound)
  std::unique_ptr<::grpc::Server> buildAndStart();

private:
  std::string m_serverAddress;
  ::grpc::ServerBuilder m_builder;
};

}  // namespace cta::grpc_server
