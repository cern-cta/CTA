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
    case DriveDownReason::TransferSessionFailed:
      return {log::ERR, "Data transfer session failed"};
    case DriveDownReason::DriveNotFound:
      return {log::ERR, "Drive not found"};
    case DriveDownReason::DriveDiscoveryFailed:
      return {log::ERR, "Drive discovery failed"};
    case DriveDownReason::DriveOpenFailed:
      return {log::ERR, "Drive open failed"};
    case DriveDownReason::CleanerFailed:
      return {log::ERR, "Cleaner failed"};
    case DriveDownReason::TapeCleanupFailed:
      return {log::ERR, "Tape cleanup failed"};
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
  return reason == formatDriveDownReason(DriveDownReason::Shutdown) || reason == "[cta-taped] Exiting cta-taped"
         || reason == "[cta-taped] INFO Exiting cta-taped" || reason == "[cta-taped] ERROR Exiting cta-taped"
         || reason == "[cta-taped] ERROR [cta-taped] Exiting cta-taped";
}
}  // namespace cta::common::dataStructures
