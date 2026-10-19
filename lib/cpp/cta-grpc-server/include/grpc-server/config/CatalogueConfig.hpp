/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>

namespace cta::grpc_server {

/**
 * @brief gRPC server-specific catalogue options.
 *
 * Complements cta::runtime::CatalogueConfig (runtime/config/CommonConfig.hpp), which holds the catalogue location.
 */
struct CatalogueConfig final {
  uint32_t number_of_connections = 10;  //!< Number of connections to the catalogue database (default: 10)

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (number_of_connections == 0) {
      result.addError("number_of_connections", "must be greater than zero");
    }
    return result;
  }
};

}  // namespace cta::grpc_server
