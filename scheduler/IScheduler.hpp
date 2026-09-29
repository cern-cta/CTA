/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"

#include <string>
#include <string_view>

namespace cta {

namespace common::dataStructures {
struct DriveInfo;
struct SecurityIdentity;
class DesiredDriveState;
}  // namespace common::dataStructures

namespace log {
class LogContext;
}

class IScheduler {
public:
  virtual ~IScheduler() = default;

  // Publish reported and desired Down, preserving an existing specific operator or failure reason.
  // Attempt both publications even after a failure, then rethrow the first exception.
  void putDriveDown(const common::dataStructures::DriveInfo& driveInfo,
                    common::dataStructures::DriveDownReason reason,
                    log::LogContext& lc,
                    std::string_view detail = {});

  virtual void ping(log::LogContext& lc) = 0;

  virtual void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                 cta::common::dataStructures::MountType type,
                                 cta::common::dataStructures::DriveStatus status,
                                 log::LogContext& lc) = 0;

  virtual void setDesiredDriveState(const std::string& driveName,
                                    const common::dataStructures::DesiredDriveState& desiredState,
                                    log::LogContext& lc) = 0;

  virtual bool checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo& driveInfo, log::LogContext& lc) = 0;

  virtual common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string& driveName,
                                                                         log::LogContext& lc) = 0;

  virtual void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                     const common::dataStructures::DesiredDriveState& desiredState,
                                     const common::dataStructures::MountType& type,
                                     const common::dataStructures::DriveStatus& status,
                                     const common::dataStructures::SecurityIdentity& identity,
                                     log::LogContext& lc) = 0;

  virtual void reportSchedulerBackendName(const std::string& driveName, log::LogContext& lc) = 0;
};

}  // namespace cta
