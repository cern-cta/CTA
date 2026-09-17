/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveUsability.hpp"

namespace cta::tape::daemon {

// Decisions needed by DriveController after a session returns.
// Transfer outcome and statistics remain in TapeSessionTracker.
struct TapeSessionResult {
  DriveUsability driveUsability = DriveUsability::Reusable;

  // Recovery requested after the session has safely finished its local work.
  bool backendRecoveryRequired = false;
  bool retryDelayRequired = false;
};

}  // namespace cta::tape::daemon
