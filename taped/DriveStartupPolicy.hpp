/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"

#include <ctime>
#include <optional>

namespace cta::tape::daemon {
struct DriveStartupConfig;

/// Registration action selected without accessing the catalogue or hardware.
struct DriveStartupDecision {
  enum class Action { Register, RecoverExistingUp, RequireOperatorUp };
  Action action = Action::Register;

  /// Initial operator intent for Register; recovery actions retain the existing catalogue entry.
  common::dataStructures::DesiredDriveState desiredState {};
};

/// Decide whether to register a drive or preserve an existing Up request for recovery.
/// Existing Down reasons and comments are retained unless the reason permits automatic startup.
/// recordedAt timestamps any newly created startup reason; preserved reasons keep their original timestamp.
DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const std::optional<common::dataStructures::TapeDrive>& previous,
                                        std::time_t recordedAt = std::time(nullptr));

}  // namespace cta::tape::daemon
