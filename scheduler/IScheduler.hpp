/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta {

namespace common::dataStructures {
struct DriveInfo;
class DesiredDriveState;
}  // namespace common::dataStructures

namespace log {
class LogContext;
}

namespace tape::daemon {
class DriveConfigEntry;
}

namespace tape::daemon::common {
struct TapedConfiguration;
}

class IScheduler {
public:
  virtual ~IScheduler() = default;

  virtual void ping(log::LogContext& lc) = 0;

  virtual void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                 cta::common::dataStructures::MountType type,
                                 cta::common::dataStructures::DriveStatus status,
                                 log::LogContext& lc) = 0;

  virtual void setDesiredDriveState(const cta::common::dataStructures::SecurityIdentity& cliIdentity,
                                    const std::string& driveName,
                                    const common::dataStructures::DesiredDriveState& desiredState,
                                    log::LogContext& lc) = 0;

  virtual bool checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo& driveInfo, log::LogContext& lc) = 0;

  virtual common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string& driveName,
                                                                         log::LogContext& lc) = 0;

  virtual void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                     const common::dataStructures::DesiredDriveState& desiredState,
                                     const common::dataStructures::MountType& type,
                                     const common::dataStructures::DriveStatus& status,
                                     const tape::daemon::DriveConfigEntry& driveConfigEntry,
                                     const common::dataStructures::SecurityIdentity& identity,
                                     log::LogContext& lc) = 0;

  virtual void reportDriveConfig(const cta::tape::daemon::DriveConfigEntry& driveConfigEntry,
                                 const cta::tape::daemon::common::TapedConfiguration& tapedConfig,
                                 log::LogContext& lc) = 0;
};

}  // namespace cta
