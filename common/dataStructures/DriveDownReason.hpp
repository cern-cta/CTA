/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <string_view>

namespace cta::common::dataStructures {

/**
 * @brief Reasons recorded by cta-taped when requesting or reporting a drive down.
 *
 * Categories identify operation context; underlying failure causes belong in the detail string.
 */
enum class DriveDownReason {
  /// The daemon is starting and the drive is not yet ready for scheduling.
  Startup,
  /// The daemon is shutting down normally.
  Shutdown,
  /// A readiness probe found a tape in the drive without automatic cleanup enabled.
  TapeDetected,
  /// A readiness probe failed to establish that the drive was empty.
  DriveProbeFailed,
  /// A transfer session could not discover, locate, or open its drive.
  SessionDriveAccessFailed,
  /// Cleanup could not establish a reusable drive, including failures to access it, reset it, or eject a tape.
  DriveCleanupFailed,
  /// Fallback for an unusable session drive when no existing specific failure or operator reason is retained.
  SessionLeftDriveUnusable
};

/**
 * @brief Get the logging severity for a drive-down reason.
 * @param reason Reason category to classify.
 * @return INFO for startup/shutdown and ERR for failures.
 * @throws std::invalid_argument If the reason is unknown.
 */
int driveDownReasonSeverity(DriveDownReason reason);

/**
 * @brief Format a drive-down reason with optional failure details.
 * @param reason Reason category to format.
 * @param detail Additional text appended when nonempty.
 * @return A string in the form @c "[cta-taped] <severity> <message>", with @c ": <detail>" appended when provided.
 * @throws std::invalid_argument If the reason is unknown.
 */
std::string formatDriveDownReason(DriveDownReason reason, std::string_view detail = {});

/**
 * @brief Check whether a formatted reason represents a clean shutdown.
 * @param reason Formatted drive-down reason to inspect.
 * @return True only for the formatted Shutdown reason without details.
 */
bool isCleanDriveShutdownReason(std::string_view reason);

}  // namespace cta::common::dataStructures
