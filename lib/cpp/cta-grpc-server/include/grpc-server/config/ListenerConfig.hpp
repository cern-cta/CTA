/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/TlsConfig.hpp"
#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace cta::grpc_server {

/**
 * @brief How the gRPC server listens for connections: port, request processing threads and TLS
 */
struct ListenerConfig final {
  uint16_t port = 50051;                 //!< Port to listen on for gRPC connections
  std::optional<int> number_of_threads;  //!< Maximum number of threads for the gRPC server
  TlsConfig tls;                         //!< TLS config (mandatory)

  /**
   * @brief Validate the listener configuration parameters
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const;

  static constexpr std::size_t memberCount() { return 3; }
};

}  // namespace cta::grpc_server
