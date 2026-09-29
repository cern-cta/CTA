/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "IScheduler.hpp"

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Logging.hpp"

#include <exception>

namespace cta {

void IScheduler::putDriveDown(const common::dataStructures::DriveInfo& driveInfo,
                              common::dataStructures::DriveDownReason reason,
                              log::LogContext& lc,
                              std::string_view detail) {
  common::dataStructures::DesiredDriveState driveState;
  driveState.reason = common::dataStructures::formatDriveDownReason(reason, detail);
  // This allows us to rethrow only the first exception we encountered
  std::exception_ptr firstFailure;
  const auto recordFailure = [&](const char* message) {
    if (!firstFailure) {
      firstFailure = std::current_exception();
    }
    try {
      std::rethrow_exception(std::current_exception());
    } catch (const std::exception& ex) {
      log::ScopedParamContainer params(lc);
      const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
      params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
      lc.log(log::ERR, message);
    } catch (...) {
      lc.log(log::ERR, message);
    }
  };

  try {
    const auto currentState = getDesiredDriveState(driveInfo.driveName, lc);
    const auto startupReason =
      common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
    if (!currentState.up && currentState.reason && !currentState.reason->empty()
        && *currentState.reason != startupReason
        && !common::dataStructures::isCleanDriveShutdownReason(*currentState.reason)) {
      // Leave the catalogue reason untouched, including operator and session failure reasons.
      driveState.reason.reset();
    }
  } catch (...) {
    driveState.reason.reset();
    recordFailure("Failed to read the existing drive-down reason.");
  }

  if (driveState.reason) {
    lc.logEvent(common::dataStructures::driveDownReasonSeverity(reason),
                *driveState.reason,
                semconv::log::EventNameValues::kPuttingTapeDriveDown);
  }

  // Failure of one publication must not prevent attempting the other.
  try {
    reportDriveStatus(driveInfo,
                      common::dataStructures::MountType::NoMount,
                      common::dataStructures::DriveStatus::Down,
                      lc);
  } catch (...) {
    recordFailure("Failed to publish the reported down status.");
  }

  try {
    setDesiredDriveState(driveInfo.driveName, driveState, lc);
  } catch (...) {
    recordFailure("Failed to publish the desired down state.");
  }

  if (firstFailure) {
    std::rethrow_exception(firstFailure);
  }
}

}  // namespace cta
