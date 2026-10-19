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
 * @brief Verification-related options of the gRPC servers (admin and workflow APIs).
 */
struct VerificationConfig final {
  std::string mount_policy = "";  //!< Mount policy used for verification requests

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

}  // namespace cta::grpc_server
