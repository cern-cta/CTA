/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"

#include <optional>

namespace cta::tape::daemon {
struct DriveStartupConfig;
struct DriveRegistrationConfig;

/// Registration action selected without accessing the catalogue or hardware.
struct DriveStartupDecision {
  enum class Action { Register, RecoverExistingUp, RequireOperatorUp };
  Action action = Action::Register;

  /// Desired state written when the catalogue entry is (re)created at startup.
  common::dataStructures::DesiredDriveState desiredState {};
};

/// @brief Decide whether to register the drive, resume an existing Up request or require a new operator request.
///
/// Preserve an existing Up request only when preserve_desired_up is enabled.
/// This takes precedence over auto_up.
/// A missing catalogue entry uses registration.desired_up, independently of the startup settings.
/// Existing desired-Down entries use config.auto_up only with the recorded daemon-shutdown reason.
/// Retain existing comments, force flags and other Down reasons when recreating an existing catalogue entry.
DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const DriveRegistrationConfig& registration,
                                        const std::optional<common::dataStructures::TapeDrive>& previous);

}  // namespace cta::tape::daemon
