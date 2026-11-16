/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/TlsConfig.hpp"
#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>

namespace cta::grpc_server {

/// @brief How the gRPC server listens for connections: port, request processing threads and TLS
struct ListenerConfig final {
  /// Port to listen on for gRPC connections
  uint16_t port = 50051;
  /// Maximum number of threads; zero selects eight per detected CPU (at least one CPU).
  int number_of_threads = 0;
  /// TLS config (mandatory)
  TlsConfig tls;

  /// @brief Validate the listener configuration parameters
  /// @return The validation result (will contain the errors if present)
  cta::runtime::ValidationResult validate() const;

  /// Resolve automatic sizing, bounding the result to the gRPC API integer range.
  int resolveThreadCount(unsigned detectedCpuCount) const;

  static constexpr std::size_t memberCount() { return 3; }
};

}  // namespace cta::grpc_server
