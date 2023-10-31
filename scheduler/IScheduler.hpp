/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>

namespace cta {

namespace common::dataStructures {
class DriveInfo;
class DesiredDriveState;
} // namespace common::dataStructures

namespace log {
class LogContext;
}

namespace tape::daemon {
class TpconfigLine;
class TapedConfiguration;
} // namespace tape::daemon

class IScheduler {
public:
  virtual ~IScheduler() = default;

  virtual void ping(log::LogContext & lc) = 0;

  virtual void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    cta::common::dataStructures::MountType type,
    cta::common::dataStructures::DriveStatus status,
    log::LogContext & lc) = 0;

  virtual void setDesiredDriveState(const cta::common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string & driveName,
    const common::dataStructures::DesiredDriveState & desiredState,
    log::LogContext & lc) = 0;

  virtual bool checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo,
    log::LogContext & lc) = 0;
  
  virtual common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string &driveName,
    log::LogContext & lc) = 0;

  virtual void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
    const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc) = 0;

  virtual void reportDriveConfig(const cta::tape::daemon::TpconfigLine& tpConfigLine,
    const cta::tape::daemon::TapedConfiguration& tapedConfig, log::LogContext& lc) = 0;
};

} // namespace cta