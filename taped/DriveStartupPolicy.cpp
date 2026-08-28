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
    decision.desiredState.up = registration.auto_up;
    decision.desiredState.reason = formatDriveDownReason(DriveDownReason::Startup);
    return decision;
  }

  // Recovery takes precedence over auto_up, including when recovery is disabled.
  if (previous->desiredUp) {
    return {.action = config.recover_existing_up ? Action::RecoverExistingUp : Action::RequireOperatorUp};
  }

  DriveStartupDecision decision;
  auto& desired = decision.desiredState;
  desired.reason = previous->reasonUpDown;
  desired.comment = previous->userComment;

  if (!desired.reason || desired.reason->empty()
      || classifyPersistedDriveReason(*desired.reason) == PersistedDriveReasonKind::CleanShutdown) {
    desired.reason = formatDriveDownReason(DriveDownReason::Startup);
    desired.up = config.auto_up;
  }
  return decision;
}

}  // namespace cta::tape::daemon
