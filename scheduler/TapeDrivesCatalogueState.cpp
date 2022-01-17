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

#include <algorithm>
#include <list>

#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/log/Logger.hpp"
#include "TapeDrivesCatalogueState.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include "version.h"

namespace cta {

TapeDrivesCatalogueState::TapeDrivesCatalogueState(catalogue::Catalogue &catalogue) : m_catalogue(catalogue) {}

void TapeDrivesCatalogueState::createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
  const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
  const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc) {
  auto tapeDriveStatus = setTapeDriveStatus(driveInfo, desiredState, type, status, tpConfigLine, identity);
  auto driveNames = m_catalogue.getTapeDriveNames();
  auto it = std::find(driveNames.begin(), driveNames.end(), tapeDriveStatus.driveName);
  if (it != driveNames.end()) {
    m_catalogue.deleteTapeDrive(tapeDriveStatus.driveName);
  }
  tapeDriveStatus.ctaVersion = CTA_VERSION;
  m_catalogue.createTapeDrive(tapeDriveStatus);
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveInfo.driveName);
  lc.log(log::DEBUG, "In TapeDrivesCatalogueState::createTapeDriveStatus(): success.");
}

void TapeDrivesCatalogueState::checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo) {
  const auto driveNames = m_catalogue.getTapeDriveNames();
  try {
    const auto tapeDrive = m_catalogue.getTapeDrive(driveInfo.driveName);
    if (!tapeDrive) return;  // tape drive does not exist
    if (tapeDrive.value().logicalLibrary != driveInfo.logicalLibrary || tapeDrive.value().host != driveInfo.host) {
      throw DriveAlreadyExistsException(std::string("The drive name=") + driveInfo.driveName +
        " logicalLibrary=" + driveInfo.logicalLibrary +
        " host=" + driveInfo.host +
        " cannot be created because a drive with a same name with logicalLibrary=" + tapeDrive.value().logicalLibrary +
        " host=" + tapeDrive.value().host +
        " already exists.");
    }
  } catch (cta::exception::Exception & ex) {
    // Drive does not exist
    // We can create it, do nothing then
  }
}

std::list<cta::common::dataStructures::TapeDrive> TapeDrivesCatalogueState::getDriveStates(
  log::LogContext & lc) const {
  std::list<cta::common::dataStructures::TapeDrive> tapeDrivesList;
  const auto driveNames = m_catalogue.getTapeDriveNames();
  for (const auto& driveName : driveNames) {
    const auto tapeDrive = m_catalogue.getTapeDrive(driveName);
    if (!tapeDrive) continue;
    tapeDrivesList.push_back(tapeDrive.value());
  }
  return tapeDrivesList;
}

void TapeDrivesCatalogueState::removeDrive(const std::string& drive, log::LogContext &lc) {
  try {
    m_catalogue.deleteTapeDrive(drive);
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In TapeDrivesCatalogueState::removeDrive(): removed tape drive from database.");
  } catch (cta::exception::Exception & ex) {
    lc.log(log::WARNING, "In TapeDrivesCatalogueState::removeDrive(): Problem to remove tape drive from database.");
  }
}

void TapeDrivesCatalogueState::setDesiredDriveState(const std::string& drive,
  const common::dataStructures::DesiredDriveState & desiredState, log::LogContext &lc) {
  common::dataStructures::DesiredDriveState newDesiredState = desiredState;
  auto driveState = m_catalogue.getTapeDrive(drive);
  if (!driveState) return;
  if(desiredState.comment){
    //In case we modify the comment, we want to keep the same status and forceDown of the drive
    newDesiredState.up = driveState.value().desiredUp;
    newDesiredState.forceDown = driveState.value().desiredForceDown;
  }
  driveState.value().desiredUp = newDesiredState.up;
  driveState.value().desiredForceDown = newDesiredState.forceDown;
  driveState.value().reasonUpDown = newDesiredState.reason;
  driveState.value().userComment = newDesiredState.comment;
  m_catalogue.modifyTapeDrive(driveState.value());
}

void TapeDrivesCatalogueState::updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo,
  const ReportDriveStatsInputs& inputs, log::LogContext & lc) {
  auto driveState = m_catalogue.getTapeDrive(driveInfo.driveName);
  if (!driveState) return;
  // Set the parameters that we always set
  driveState.value().host = driveInfo.host;
  driveState.value().logicalLibrary = driveInfo.logicalLibrary;

  switch (driveState.value().driveStatus) {
    case common::dataStructures::DriveStatus::Transferring:
    {
      const time_t timeDifference = inputs.reportTime - driveState.value().lastModificationLog.value().time;
      uint64_t bytesTransferedInSession = driveState.value().bytesTransferedInSession ?
        driveState.value().bytesTransferedInSession.value() : 0;
      const uint64_t bytesDifference = inputs.bytesTransferred - bytesTransferedInSession;
      driveState.value().lastModificationLog = common::dataStructures::EntryLog(
        "NO_USER", driveInfo.host, inputs.reportTime);
      driveState.value().bytesTransferedInSession = inputs.bytesTransferred;
      driveState.value().filesTransferedInSession = inputs.filesTransferred;
      auto latestBandwidth = timeDifference ? 1.0 * bytesDifference / timeDifference : 0.0;
      driveState.value().latestBandwidth = std::to_string(latestBandwidth);
      break;
    }
    default:
      return;
  }
  m_catalogue.modifyTapeDrive(driveState.value());
}

void TapeDrivesCatalogueState::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
  time_t reportTime, log::LogContext & lc, uint64_t mountSessionId, uint64_t byteTransferred,
  uint64_t filesTransferred, double latestBandwidth, const std::string& vid,
  const std::string& tapepool, const std::string & vo) {
  using common::dataStructures::DriveStatus;
  // Wrap all the parameters together for easier manipulation by sub-functions
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.byteTransferred = byteTransferred;
  inputs.filesTransferred = filesTransferred;
  inputs.latestBandwidth = latestBandwidth;
  inputs.mountSessionId = mountSessionId;
  inputs.reportTime = reportTime;
  inputs.status = status;
  inputs.vid = vid;
  inputs.tapepool = tapepool;
  inputs.vo = vo;
  updateDriveStatus(driveInfo, inputs, lc);
}

void TapeDrivesCatalogueState::updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  const ReportDriveStatusInputs& inputs, log::LogContext &lc) {
  // First, get the drive state.
  const auto driveStateOptional = m_catalogue.getTapeDrive(driveInfo.driveName);
  if (!driveStateOptional) return;
  auto driveState = driveStateOptional.value();
  // Set the parameters that we always set
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;
  // Keep track of previous status to log changes
  auto previousStatus = driveState.driveStatus;
  // Set the status
  switch (inputs.status) {
    case common::dataStructures::DriveStatus::Down:
      setDriveDown(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Up:
      setDriveUpOrMaybeDown(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Probing:
      setDriveProbing(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Starting:
      setDriveStarting(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Mounting:
      setDriveMounting(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Transferring:
      setDriveTransfering(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Unloading:
      setDriveUnloading(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Unmounting:
      setDriveUnmounting(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::DrainingToDisk:
      setDriveDrainingToDisk(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::CleaningUp:
      setDriveCleaningUp(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Shutdown:
      setDriveShutdown(driveState, inputs);
      break;
    default:
      throw exception::Exception("Unexpected status in DriveRegister::reportDriveStatus");
  }
  // If the drive is a state incompatible with space reservation, make sure there is none:
  switch (driveState.driveStatus) {
    case common::dataStructures::DriveStatus::Down:
    case common::dataStructures::DriveStatus::Shutdown:
    case common::dataStructures::DriveStatus::Unknown:
    case common::dataStructures::DriveStatus::Up:
    {
      if (!driveState.diskSystemName.empty()) {
        log::ScopedParamContainer params(lc);
        params.add("diskSystem", driveState.diskSystemName)
              .add("bytes", driveState.reservedBytes)
              .add("previousStatus", toString(previousStatus))
              .add("newStatus", toString(driveState.driveStatus));
        lc.log(log::WARNING, "In TapeDrivesCatalogueState::updateDriveStatus(): will clear non-empty disk space reservation on status change.");
        driveState.diskSystemName = "";
        driveState.reservedBytes = 0;
      }
    }
    default:
      break;
  }
  if (previousStatus != driveState.driveStatus) {
    log::ScopedParamContainer params(lc);
    params.add("oldStatus", toString(previousStatus))
          .add("newStatus", toString(driveState.driveStatus));
    lc.log(log::INFO, "In TapeDrivesCatalogueState::updateDriveStatus(): changing drive status.");
  }
  m_catalogue.modifyTapeDrive(driveState);
}

void TapeDrivesCatalogueState::setDriveDown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already down, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Down) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId = 0;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = inputs.reportTime;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = common::dataStructures::DriveStatus::Down;
  driveState.desiredUp = false;
  driveState.desiredForceDown = false;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = nullopt_t();
  if (inputs.reason) driveState.reasonUpDown = inputs.reason;
}

void TapeDrivesCatalogueState::setDriveUpOrMaybeDown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // Decide whether we should be up or down
  auto targetStatus = common::dataStructures::DriveStatus::Up;
  if (!driveState.desiredUp) {
    driveState.driveStatus = common::dataStructures::DriveStatus::Down;
    if (inputs.reason) driveState.reasonUpDown = inputs.reason;
  }
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == targetStatus) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId = 0;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = inputs.reportTime;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = targetStatus;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = nullopt_t();
}

void TapeDrivesCatalogueState::setDriveProbing(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // If we were already up (or down), then we only update the last update time.
  if (driveState.driveStatus == inputs.status) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId = 0;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = inputs.reportTime;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = inputs.status;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = nullopt_t();
}

void TapeDrivesCatalogueState::setDriveStarting(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Starting) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = inputs.reportTime;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.startStartTime = inputs.reportTime;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Starting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
  driveState.currentActivity = inputs.activity;
}

void TapeDrivesCatalogueState::setDriveMounting(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already starting, then we only update the last update time.
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Mounting) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  // driveState.sessionstarttime = inputs.reportTime;
  driveState.mountStartTime = inputs.reportTime;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Mounting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveTransfering(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we were already transferring, we update the full statistics
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Transferring) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    driveState.bytesTransferedInSession = inputs.byteTransferred;
    driveState.filesTransferedInSession = inputs.filesTransferred;
    driveState.latestBandwidth = std::to_string(inputs.latestBandwidth);
    return;
  }
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = inputs.byteTransferred;
  driveState.filesTransferedInSession = inputs.filesTransferred;
  driveState.latestBandwidth = std::to_string(inputs.latestBandwidth);
  // driveState.sessionstarttime = inputs.reportTime;
  // driveState.mountstarttime = inputs.reportTime;
  driveState.transferStartTime = inputs.reportTime;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Transferring;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveUnloading(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unloading) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = inputs.reportTime;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Unloading;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveUnmounting(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Unmounting) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = inputs.reportTime;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Unmounting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveDrainingToDisk(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::DrainingToDisk) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = inputs.reportTime;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::DrainingToDisk;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveCleaningUp(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::CleaningUp) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = inputs.reportTime;
  driveState.shutdownTime = 0;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentActivity = nullopt_t();
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveShutdown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  if (driveState.driveStatus == common::dataStructures::DriveStatus::Shutdown) {
    driveState.lastModificationLog = common::dataStructures::EntryLog(
      "NO_USER", driveState.host, inputs.reportTime);
    return;
  }
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = 0;
  driveState.bytesTransferedInSession = 0;
  driveState.filesTransferedInSession = 0;
  driveState.latestBandwidth = "0";
  driveState.sessionStartTime = 0;
  driveState.mountStartTime = 0;
  driveState.transferStartTime = 0;
  driveState.unloadStartTime = 0;
  driveState.unmountStartTime = 0;
  driveState.drainingStartTime = 0;
  driveState.downOrUpStartTime = 0;
  driveState.probeStartTime = 0;
  driveState.cleanupStartTime = 0;
  driveState.shutdownTime = inputs.reportTime;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentActivity = nullopt_t();
  driveState.currentVo = inputs.vo;
}

common::dataStructures::TapeDrive TapeDrivesCatalogueState::setTapeDriveStatus(
  const common::dataStructures::DriveInfo& driveInfo,
  const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
  const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
  const common::dataStructures::SecurityIdentity& identity) {
  const time_t reportTime = time(nullptr);
  common::dataStructures::TapeDrive tapeDriveStatus;
  tapeDriveStatus.driveName = driveInfo.driveName;
  tapeDriveStatus.host = driveInfo.host;
  tapeDriveStatus.logicalLibrary = driveInfo.logicalLibrary;
  tapeDriveStatus.latestBandwidth = "0.0";
  tapeDriveStatus.downOrUpStartTime = reportTime;
  tapeDriveStatus.mountType = type;
  tapeDriveStatus.driveStatus = status;
  tapeDriveStatus.desiredUp = desiredState.up;
  tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  if (desiredState.reason) tapeDriveStatus.reasonUpDown = desiredState.reason;
  if (desiredState.comment) tapeDriveStatus.userComment = desiredState.comment;
  tapeDriveStatus.diskSystemName = "NULL";
  tapeDriveStatus.reservedBytes = 0;
  tapeDriveStatus.devFileName = tpConfigLine.devFilename;
  tapeDriveStatus.rawLibrarySlot = tpConfigLine.rawLibrarySlot;
  if (identity.username.empty()) {
    tapeDriveStatus.creationLog = common::dataStructures::EntryLog("NO_USER", driveInfo.host, reportTime);
    tapeDriveStatus.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveInfo.host, reportTime);
  } else {
    tapeDriveStatus.creationLog = common::dataStructures::EntryLog(identity.username, identity.host, reportTime);
    tapeDriveStatus.lastModificationLog = common::dataStructures::EntryLog(identity.username, identity.host,
      reportTime);
  }
  return tapeDriveStatus;
}

}  // namespace cta
