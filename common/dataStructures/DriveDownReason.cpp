/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "DriveDownReason.hpp"

#include "common/log/Logger.hpp"

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
      return {log::INFO, "Daemon started"};
    case DriveDownReason::StartupRecoveryDisabled:
      return {log::INFO, "Daemon started: desired Up detected, but startup recovery is disabled"};
    case DriveDownReason::Shutdown:
      return {log::INFO, "Daemon shutting down"};
    case DriveDownReason::TapeDetected:
      return {log::ERR, "Tape detected in drive"};
    case DriveDownReason::DriveProbeFailed:
      return {log::ERR, "Drive probe failed"};
    case DriveDownReason::SessionDriveAccessFailed:
      return {log::ERR, "Drive access failed"};
    case DriveDownReason::DriveCleanupFailed:
      return {log::ERR, "Drive cleanup failed"};
    case DriveDownReason::SessionLeftDriveUnusable:
      return {log::ERR, "Drive reuse unsafe"};
    case DriveDownReason::UnexpectedFailure:
      return {log::ERR, "Unexpected failure"};
    case DriveDownReason::SessionDidNotStopSafely:
      return {log::ERR, "Session did not stop safely"};
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
  result += description.message;
  if (!detail.empty()) {
    result += ": ";
    result += detail;
  }
  return result;
}

PersistedDriveReasonKind classifyPersistedDriveReason(std::string_view reason) {
  using Kind = PersistedDriveReasonKind;
  // Match plain startup exactly: disabled recovery must retain its specific Down reason.
  if (reason == formatDriveDownReason(DriveDownReason::Startup)) {
    return Kind::Startup;
  }
  const auto shutdown = formatDriveDownReason(DriveDownReason::Shutdown);
  if (reason == shutdown || reason.starts_with(shutdown + ": ")) {
    return Kind::CleanShutdown;
  }
  return Kind::Other;
}
}  // namespace cta::common::dataStructures
