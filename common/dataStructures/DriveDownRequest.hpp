/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveDownReason.hpp"

namespace cta::common::dataStructures {

/// Explicit commands replace reasons; daemon requests preserve an existing specific reason.
enum class ExistingReasonPolicy { Replace, PreserveSpecific };

struct DriveDownRequest {
  std::string reason;
  bool force = false;
  ExistingReasonPolicy reasonPolicy = ExistingReasonPolicy::Replace;

  /// Format a daemon reason and apply its non-overwriting policy.
  static DriveDownRequest forDaemon(DriveDownReason reason, std::string_view detail = {}) {
    return {.reason = formatDriveDownReason(reason, detail), .reasonPolicy = ExistingReasonPolicy::PreserveSpecific};
  }
};

}  // namespace cta::common::dataStructures
