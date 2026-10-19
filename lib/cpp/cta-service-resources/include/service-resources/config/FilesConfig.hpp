/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>

namespace cta::service_resources {

/**
 * @brief Options related to how the frontend handles tape file copies and the recycle log.
 */
struct FilesConfig final {
  uint64_t missing_file_copies_min_age_secs = 0;  //!< Minimum age of missing tape file copies before they are reported
  uint64_t recycle_log_quarantine_secs =
    31 * 24 * 60 * 60;  //! Minimum quarantine period before a tape can be reclaimed (default: 31 days).

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const { return {}; }
};

}  // namespace cta::service_resources
