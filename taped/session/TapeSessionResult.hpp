/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

/**
 * @brief Decisions needed by DriveController after a tape session returns.
 */
struct TapeSessionResult {
  /**
   * @brief Whether the drive is still usable after the tape session.
   * False means the drive must remain down, for example after a cleanup failure.
   */
  bool driveReusable = true;

  /**
   * @brief True if the completed session has no recorded failures, including final reporting failures.
   */
  bool successful = true;
};

}  // namespace cta::tape::daemon
