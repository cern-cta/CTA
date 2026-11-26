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

#ifndef CTA_PGSCHED

/// @brief Service-specific scheduler options (objectstore scheduler only), read from `[scheduler.client]`.
struct SchedulerClientConfig final {
  /// Scheduler DB thread pool size (REQUIRED, must be > 0)
  uint32_t number_of_threads = 0;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (number_of_threads == 0) {
      result.addError("number_of_threads", "must be greater than zero");
    }
    return result;
  }
};

/// @brief Full `[scheduler]` section of the services: scheduler DB location plus client options.
using ServiceSchedulerConfig = cta::runtime::BasicSchedulerConfig<SchedulerClientConfig>;

#else

/// @brief Full `[scheduler]` section of the services (no client options for the PostgreSQL scheduler).
using ServiceSchedulerConfig = cta::runtime::BasicSchedulerConfig<>;

#endif

}  // namespace cta::service_resources
