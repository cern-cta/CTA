/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <string_view>

namespace cta::common::dataStructures {

// Reasons recorded by cta-taped when requesting or reporting a drive down.
// Categories identify operation context; underlying failure causes belong in the detail string.
enum class DriveDownReason {
  // The daemon is starting and the drive is not yet ready for scheduling.
  Startup,
  // The daemon is shutting down normally.
  Shutdown,
  // A readiness probe found a tape in the drive without automatic cleanup enabled.
  TapeDetected,
  // A readiness probe failed to establish that the drive was empty.
  DriveProbeFailed,
  // A transfer session could not discover, locate, or open its drive.
  SessionDriveAccessFailed,
  // Cleanup could not establish a reusable drive, including failures to access it, reset it, or eject a tape.
  DriveCleanupFailed,
  // Fallback for an unusable session drive when no existing specific failure or operator reason is retained.
  SessionLeftDriveUnusable
};

// Return INFO for startup/shutdown and ERR for failures; throw std::invalid_argument for an unknown reason.
int driveDownReasonSeverity(DriveDownReason reason);

// Format "[cta-taped] <severity> <message>", appending ": <detail>" when detail is nonempty.
// Throw std::invalid_argument for an unknown reason.
std::string formatDriveDownReason(DriveDownReason reason, std::string_view detail = {});

// Match only the formatted Shutdown reason without details.
bool isCleanDriveShutdownReason(std::string_view reason);

}  // namespace cta::common::dataStructures
