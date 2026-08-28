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

  /// Initial operator intent for Register; recovery actions retain the existing catalogue entry.
  common::dataStructures::DesiredDriveState desiredState {};
};

/// @brief Decide whether to register the drive, resume an existing Up request or require a new operator request.
///
/// An existing Up request takes precedence over auto_up; resume it only when recover_existing_up is enabled.
/// A missing catalogue entry uses registration.auto_up, independently of the startup settings.
/// Existing desired-Down entries use config.auto_up only with an empty or clean-shutdown reason.
/// Retain existing comments and other Down reasons even when recreating an existing catalogue entry.
DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const DriveRegistrationConfig& registration,
                                        const std::optional<common::dataStructures::TapeDrive>& previous);

}  // namespace cta::tape::daemon
