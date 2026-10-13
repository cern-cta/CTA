/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveStartupPolicy.hpp"

#include "TapedConfig.hpp"
#include "common/dataStructures/DriveDownReason.hpp"

namespace cta::tape::daemon {

DriveStartupDecision decideDriveStartup(const DriveStartupConfig& config,
                                        const std::optional<common::dataStructures::TapeDrive>& previous) {
  using namespace common::dataStructures;
  using Action = DriveStartupDecision::Action;

  // Recovery takes precedence over auto_up, including when recovery is disabled.
  if (previous && previous->desiredUp) {
    return {.action = config.recover_existing_up ? Action::RecoverExistingUp : Action::RequireOperatorUp};
  }

  DriveStartupDecision decision;
  auto& desired = decision.desiredState;
  if (previous) {
    desired.reason = previous->reasonUpDown;
    desired.comment = previous->userComment;
  }

  if (!desired.reason || desired.reason->empty() || isCleanDriveShutdownReason(*desired.reason)) {
    desired.reason = formatDriveDownReason(DriveDownReason::Startup);
    desired.up = config.auto_up;
  }
  return decision;
}

}  // namespace cta::tape::daemon
