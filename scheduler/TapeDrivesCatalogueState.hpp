/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <limits>
#include <string>

#include "catalogue/Catalogue.hpp"

namespace cta {
namespace log {
class LogContext;
}

namespace common {
namespace dataStructures {
class DesiredDriveState;
class DriveInfo;
class TapeDrive;
}
}

namespace tape {
namespace daemon {
class TpconfigLine;
}
}

struct ReportDriveStatusInputs {
  common::dataStructures::DriveStatus status;
  cta::common::dataStructures::MountType mountType;
  time_t reportTime;
  uint64_t mountSessionId;
  uint64_t byteTransferred;
  uint64_t filesTransferred;
  double latestBandwidth;
  std::string vid;
  std::string tapepool;
  std::string vo;
  optional<std::string> activity;
  optional<std::string> reason;
};

struct ReportDriveStatsInputs {
  time_t reportTime;
  uint64_t bytesTransferred;
  uint64_t filesTransferred;
};

class TapeDrivesCatalogueState {
public:
  TapeDrivesCatalogueState(catalogue::Catalogue &catalogue);
  ~TapeDrivesCatalogueState() = default;
  void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
    const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc);
  CTA_GENERATE_EXCEPTION_CLASS(DriveAlreadyExistsException);
  void checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo);
  std::list<cta::common::dataStructures::TapeDrive> getDriveStates(log::LogContext & lc) const;
  void removeDrive(const std::string& drive, log::LogContext &lc);
  void setDesiredDriveState(const std::string& drive, const common::dataStructures::DesiredDriveState & desiredState,
    log::LogContext &lc);
  void updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatsInputs& inputs,
    log::LogContext & lc);
  void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
    time_t reportTime, log::LogContext & lc,
    uint64_t mountSessionId = std::numeric_limits<uint64_t>::max(),
    uint64_t byteTransferred = std::numeric_limits<uint64_t>::max(),
    uint64_t filesTransferred = std::numeric_limits<uint64_t>::max(),
    double latestBandwidth = std::numeric_limits<double>::max(),
    const std::string& vid = "",
    const std::string& tapepool = "",
    const std::string & vo = "");
  void updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatusInputs& inputs,
    log::LogContext &lc);

private:
  cta::catalogue::Catalogue &m_catalogue;

  common::dataStructures::TapeDrive setTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
    const common::dataStructures::SecurityIdentity& identity);
  void setDriveDown(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveUpOrMaybeDown(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveProbing(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveStarting(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveMounting(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveTransfering(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveUnloading(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveUnmounting(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveDrainingToDisk(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveCleaningUp(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
  void setDriveShutdown(common::dataStructures::TapeDrive & driveState, const ReportDriveStatusInputs & inputs);
};

}  // namespace cta
