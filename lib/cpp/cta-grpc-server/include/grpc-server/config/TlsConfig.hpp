/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <optional>
#include <string>

namespace cta::grpc_server {

/**
 * @brief TLS Config
 */
struct TlsConfig final {
  std::string server_key_path;                                //!< The path to the private key file (REQUIRED)
  std::string server_cert_path;                               //!< The path to the certificate file (REQUIRED)
  std::optional<std::string> chain_cert_path = std::nullopt;  //!< The path to the CA chain file (optional)

  /**
   * @brief Validate the TLS configuration parameters
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const;

  static constexpr std::size_t memberCount() { return 3; }
};

}  // namespace cta::grpc_server
