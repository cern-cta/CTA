/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace cta::service_resources {

#ifndef CTA_PGSCHED

/// @brief Service-specific scheduler options (objectstore scheduler only).
struct SchedulerConfig final {
  uint32_t number_of_threads = 0;                //!< Scheduler DB thread pool size (REQUIRED, must be > 0)
  std::optional<uint32_t> thread_stack_size_mb;  //!< Scheduler DB thread stack size (MB)

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    if (number_of_threads == 0) {
      result.addError("number_of_threads", "must be greater than zero");
    }
    if (thread_stack_size_mb.has_value() && *thread_stack_size_mb <= 0) {
      result.addError("thread_stack_size_mb", "must be greater than zero");
    }
    return result;
  }
};

#endif

}  // namespace cta::service_resources
