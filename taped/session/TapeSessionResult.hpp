/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveUsability.hpp"

namespace cta::tape::daemon {

/**
 * @brief Decisions needed by DriveController after a tape session returns.
 */
struct TapeSessionResult {
  /**
   * @brief Whether the drive is still usable after the tape session. 
   * If e.g. tape session cleanup failed, this may be set to MustRemainDown.
   */
  DriveUsability driveUsability = DriveUsability::Reusable;

  /**
   * @brief Whether to wait for backend recovery after local session work has finished.
   * Can be set due to e.g. lost backend connections.
   */
  bool backendRecoveryRequired = false;

  /**
   * @brief Whether to delay the next scheduling attempt when the drive remains reusable.
   */
  bool retryDelayRequired = false;
};

}  // namespace cta::tape::daemon
