/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"
#include "common/log/PriorityMaps.hpp"

#include <stdexcept>

namespace cta::common::dataStructures {
namespace {
// Keep severity and message together in a single mapping for logging and formatting.
struct ReasonDescription {
  int severity;
  std::string_view message;
};

ReasonDescription describe(DriveDownReason reason) {
  switch (reason) {
    case DriveDownReason::Startup:
      return {log::INFO, "Startup"};
    case DriveDownReason::Shutdown:
      return {log::INFO, "Shutdown"};
    case DriveDownReason::TapeDetected:
      return {log::ERR, "Tape detected in drive"};
    case DriveDownReason::DriveProbeFailed:
      return {log::ERR, "Drive probe failed"};
    case DriveDownReason::SessionDriveAccessFailed:
      return {log::ERR, "Session drive access failed"};
    case DriveDownReason::DriveCleanupFailed:
      return {log::ERR, "Drive cleanup failed"};
    case DriveDownReason::SessionLeftDriveUnusable:
      return {log::ERR, "Session left drive unusable"};
  }
  throw std::invalid_argument("Unknown drive-down reason");
}
}  // namespace

int driveDownReasonSeverity(DriveDownReason reason) {
  return describe(reason).severity;
}

std::string formatDriveDownReason(DriveDownReason reason, std::string_view detail) {
  const auto description = describe(reason);
  std::string result = "[cta-taped] ";
  result += log::PriorityMaps::getPriorityText(description.severity);
  result += " ";
  result += description.message;
  if (!detail.empty()) {
    result += ": ";
    result += detail;
  }
  return result;
}

bool isCleanDriveShutdownReason(std::string_view reason) {
  return reason == formatDriveDownReason(DriveDownReason::Shutdown);
}
}  // namespace cta::common::dataStructures
