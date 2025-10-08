/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <limits>
#include <optional>
#include <string>

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/exception/UserError.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace log {
class LogContext;
}

namespace common::dataStructures {
class DesiredDriveState;
struct DriveInfo;
struct TapeDrive;
struct SecurityIdentity;
}

namespace tape::daemon {
class DriveConfigEntry;
}

struct ReportDriveStatusInputs {
  common::dataStructures::DriveStatus status;
  cta::common::dataStructures::MountType mountType;
  time_t reportTime;
  uint64_t mountSessionId;
  uint64_t byteTransferred;
  uint64_t filesTransferred;
  std::string vid;
  std::string tapepool;
  std::string vo;
  std::optional<std::string> activity;
  std::optional<std::string> reason;
};

struct ReportDriveStatsInputs {
  time_t reportTime;
  uint64_t bytesTransferred;
  uint64_t filesTransferred;
};

class TapeDrivesCatalogueState {
public:
  explicit TapeDrivesCatalogueState(catalogue::Catalogue &catalogue);
  ~TapeDrivesCatalogueState() = default;
  void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::DriveConfigEntry& driveConfigEntry,
    const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc);
  CTA_GENERATE_EXCEPTION_CLASS(DriveAlreadyExistsException);
  void checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo);
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
    std::string_view vid = "",
    std::string_view tapepool = "",
    std::string_view vo = "");
  void updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo, const ReportDriveStatusInputs& inputs,
    log::LogContext &lc);

private:
  cta::catalogue::Catalogue &m_catalogue;

  common::dataStructures::TapeDrive setTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::DriveConfigEntry& driveConfigEntry,
    const common::dataStructures::SecurityIdentity& identity) const;
  void setDriveDown(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUpOrMaybeDown(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& input) const;
  void setDriveProbing(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveStarting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveMounting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveTransfering(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUnloading(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveUnmounting(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveDrainingToDisk(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveCleaningUp(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
  void setDriveShutdown(common::dataStructures::TapeDrive& driveState, const ReportDriveStatusInputs& inputs) const;
};

} // namespace cta
