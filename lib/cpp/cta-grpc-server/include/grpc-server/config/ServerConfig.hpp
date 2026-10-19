/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthConfig.hpp"
#include "grpc-server/config/ListenerConfig.hpp"
#include "runtime/config/ValidationResult.hpp"

#include <cstddef>

namespace cta::grpc_server {

/**
 * @brief Top-level gRPC config for a CTA frontend API, generic on specific auth config
 */
template<typename AuthT>
struct ServerConfig final {
  ListenerConfig grpc;  //!< Listener config parameters
  AuthT auth;           //!< Authentication configuration

  /**
   * @brief Validate the gRPC configuration for consistency
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("grpc", grpc.validate());
    result.merge("auth", auth.validate());
    return result;
  }

  static constexpr std::size_t memberCount() { return 2; }
};

using AdminServerConfig = ServerConfig<AdminAuthConfig>;
using WfeServerConfig = ServerConfig<WfeAuthConfig>;

}  // namespace cta::grpc_server
