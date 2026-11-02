/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/TlsConfig.hpp"

namespace cta::grpc_server {

cta::runtime::ValidationResult TlsConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (server_key_path.empty()) {
    result.addError("server_key_path", "cannot be empty");
  }
  if (server_cert_path.empty()) {
    result.addError("server_cert_path", "cannot be empty");
  }
  if (chain_cert_path.has_value() && chain_cert_path->empty()) {
    result.addError("chain_cert_path", "cannot be empty when provided");
  }
  return result;
}

}  // namespace cta::grpc_server
