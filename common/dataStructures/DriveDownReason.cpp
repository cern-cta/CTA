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
// Codes are persisted compatibility identifiers; display messages may change independently.
struct ReasonDescription {
  std::string_view code;
  int severity;
  std::string_view message;
};

ReasonDescription describe(DriveDownReason reason) {
  switch (reason) {
    case DriveDownReason::Startup:
      return {"startup", log::INFO, "Startup"};
    case DriveDownReason::StartupRecoveryDisabled:
      return {"startup-recovery-disabled", log::INFO, "Automatic startup recovery disabled; operator Up required"};
    case DriveDownReason::Shutdown:
      return {"shutdown", log::INFO, "Shutdown"};
    case DriveDownReason::TapeDetected:
      return {"tape-detected", log::ERR, "Tape detected in drive"};
    case DriveDownReason::DriveProbeFailed:
      return {"drive-probe-failed", log::ERR, "Drive probe failed"};
    case DriveDownReason::SessionDriveAccessFailed:
      return {"session-drive-access-failed", log::ERR, "Session drive access failed"};
    case DriveDownReason::DriveCleanupFailed:
      return {"drive-cleanup-failed", log::ERR, "Drive cleanup failed"};
    case DriveDownReason::SessionLeftDriveUnusable:
      return {"session-left-drive-unusable", log::ERR, "Session left drive unusable"};
    case DriveDownReason::UnexpectedFailure:
      return {"unexpected-failure", log::ERR, "Unexpected failure"};
    case DriveDownReason::SessionDidNotStopSafely:
      return {"session-did-not-stop-safely", log::ERR, "Session did not stop safely"};
  }
  throw std::invalid_argument("Unknown drive-down reason");
}
}  // namespace

int driveDownReasonSeverity(DriveDownReason reason) {
  return describe(reason).severity;
}

std::string formatDriveDownReason(DriveDownReason reason, std::string_view detail, std::time_t recordedAt) {
  const auto description = describe(reason);
  // Record local time without a timezone suffix.
  std::tm localTime {};
  char timestamp[64];
  if (!::localtime_r(&recordedAt, &localTime) || !std::strftime(timestamp, sizeof(timestamp), "%FT%T", &localTime)) {
    throw std::runtime_error("Cannot format drive-down reason timestamp");
  }
  std::string result = "[cta-taped:";
  result += description.code;
  result += "] ";
  result += timestamp;
  result += " ";
  result += log::PriorityMaps::getPriorityText(description.severity);
  result += " ";
  result += description.message;
  if (!detail.empty()) {
    result += ": ";
    result += detail;
  }
  return result;
}

PersistedDriveReasonKind classifyPersistedDriveReason(std::string_view reason) {
  using Kind = PersistedDriveReasonKind;
  // Only the stable prefix carries policy meaning; the rest is display text.
  if (reason.starts_with("[cta-taped:startup]")) {
    return Kind::Startup;
  }
  if (reason.starts_with("[cta-taped:shutdown]")) {
    return Kind::CleanShutdown;
  }
  return Kind::Other;
}
}  // namespace cta::common::dataStructures
