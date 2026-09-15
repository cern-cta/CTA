/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <string_view>

namespace cta::common::dataStructures {

enum class DriveDownReason {
  Startup,
  Shutdown,
  TapeDetected,
  DriveProbeFailed,
  TransferSessionFailed,
  DriveNotFound,
  DriveDiscoveryFailed,
  DriveOpenFailed,
  CleanerFailed,
  TapeCleanupFailed
};

int driveDownReasonSeverity(DriveDownReason reason);
std::string formatDriveDownReason(DriveDownReason reason, std::string_view detail = {});
bool isCleanDriveShutdownReason(std::string_view reason);

}  // namespace cta::common::dataStructures
