/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveDownReason.hpp"

#include <optional>
#include <string>

namespace cta::tape::daemon {

/// Outcomes used by DriveSession for recovery and scheduling after a tape session returns.
struct TapeSessionResult {
  /// @brief Whether the drive is still usable after the tape session.
  /// True permits scheduling without another probe: the session left the drive empty and reusable.
  /// False means the drive must remain down, for example after a cleanup failure.
  bool driveReusable = true;

  /// True if the completed session has no recorded failures, including final reporting failures.
  bool successful = true;

  /// Published by DriveSession when the outcome requires ending the reservation.
  std::optional<common::dataStructures::DriveDownReason> downReason = std::nullopt;
  std::string downDetail {};
};

}  // namespace cta::tape::daemon
