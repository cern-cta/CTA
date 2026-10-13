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

/// @brief Decide whether to register the drive, resume an existing Up request or require a new operator request.
///
/// An existing Up request takes precedence over auto_up; resume it only when recover_existing_up is enabled.
/// For registration, retain comments and Down reasons other than an empty reason or a clean-shutdown reason.
/// Use recordedAt for any new startup reason; retained reasons keep their original timestamp.
DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const std::optional<common::dataStructures::TapeDrive>& previous,
                                        std::time_t recordedAt = std::time(nullptr));

}  // namespace cta::tape::daemon
