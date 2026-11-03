/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveStartupPolicy.hpp"

#include "TapedConfig.hpp"
#include "common/dataStructures/DriveDownReason.hpp"

namespace cta::tape::daemon {

DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const DriveRegistrationConfig& registration,
                                        const std::optional<common::dataStructures::TapeDrive>& previous) {
  using namespace common::dataStructures;
  using Action = DriveStartupDecision::Action;

  // Only first registration uses the registration policy; existing entries use startup policy.
  if (!previous) {
    DriveStartupDecision decision;
    decision.desiredState.up = registration.desired_up;
    decision.desiredState.reason = formatDriveDownReason(DriveDownReason::Startup);
    return decision;
  }

  DriveStartupDecision decision;
  auto& desired = decision.desiredState;
  desired.forceDown = previous->desiredForceDown;
  desired.reason = previous->reasonUpDown;
  desired.comment = previous->userComment;

  // Recovery takes precedence over auto_up, including when recovery is disabled.
  if (previous->desiredUp) {
    if (config.preserve_desired_up) {
      decision.action = Action::RecoverExistingUp;
      desired.up = true;
    } else {
      decision.action = Action::RequireOperatorUp;
      desired.forceDown = false;
      desired.reason = formatDriveDownReason(DriveDownReason::StartupRecoveryDisabled);
    }
    return decision;
  }

  if (desired.reason && classifyPersistedDriveReason(*desired.reason) == PersistedDriveReasonKind::CleanShutdown) {
    desired.reason = formatDriveDownReason(DriveDownReason::Startup);
    desired.up = config.auto_up;
  }
  return decision;
}

}  // namespace cta::tape::daemon
