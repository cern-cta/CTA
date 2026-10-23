/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/CommonConfig.hpp"
#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>

namespace cta::service_resources {

/**
 * @brief Service-specific catalogue options.
 *
 * Read from `[catalogue.client]`, next to the catalogue location (see cta::runtime::BasicCatalogueConfig).
 */
struct CatalogueClientConfig final {
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

/// @brief Full `[catalogue]` section of the services: catalogue location plus client options.
using ServiceCatalogueConfig = cta::runtime::BasicCatalogueConfig<CatalogueClientConfig>;

}  // namespace cta::service_resources
