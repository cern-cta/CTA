/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <string>

namespace cta::grpc_server {

/**
 * @brief Identity of the CTA instance served by the gRPC server.
 *
 * Used to validate authentication (JWT audience/Kerberos principal checks, mTLS instance mapping) and as the
 * telemetry service namespace
 */
struct InstanceConfig final {
  std::string name;  //!< Name of the CTA instance (REQUIRED)

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (name.empty()) {
      result.addError("name", "cannot be empty");
    }
    return result;
  }
};

}  // namespace cta::grpc_server
