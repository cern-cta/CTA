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
  /// Whether the drive was left empty and reusable without another probe; false requires keeping it Down.
  bool driveReusable = true;

  /// True if the completed session has no recorded failures, including final reporting failures.
  bool successful = true;

  /// Published by DriveSession when the outcome requires ending the reservation.
  std::optional<common::dataStructures::DriveDownReason> downReason = std::nullopt;
  std::string downDetail {};
};

}  // namespace cta::tape::daemon
