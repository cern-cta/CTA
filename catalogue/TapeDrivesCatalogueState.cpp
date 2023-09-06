/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <algorithm>
#include <list>

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include "version.h"

namespace cta {

TapeDrivesCatalogueState::TapeDrivesCatalogueState(catalogue::Catalogue &catalogue) : m_catalogue(catalogue) {}

void TapeDrivesCatalogueState::createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
  const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
  const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc) {
  auto tapeDriveStatus = setTapeDriveStatus(driveInfo, desiredState, type, status, tpConfigLine, identity);
  auto driveNames = m_catalogue.DriveState()->getTapeDriveNames();
  auto it = std::find(driveNames.begin(), driveNames.end(), tapeDriveStatus.driveName);
  if (it != driveNames.end()) {
    m_catalogue.DriveState()->deleteTapeDrive(tapeDriveStatus.driveName);
  }
  tapeDriveStatus.ctaVersion = CTA_VERSION;
  m_catalogue.DriveState()->createTapeDrive(tapeDriveStatus);
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveInfo.driveName);
  lc.log(log::DEBUG, "In TapeDrivesCatalogueState::createTapeDriveStatus(): success.");
}

void TapeDrivesCatalogueState::checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo) {
  const auto driveNames = m_catalogue.DriveState()->getTapeDriveNames();
  try {
    const auto tapeDrive = m_catalogue.DriveState()->getTapeDrive(driveInfo.driveName);
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

void TapeDrivesCatalogueState::removeDrive(const std::string& drive, log::LogContext &lc) {
  try {
    m_catalogue.DriveState()->deleteTapeDrive(drive);
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In TapeDrivesCatalogueState::removeDrive(): removed tape drive from database.");
  } catch (cta::exception::Exception & ex) {
    lc.log(log::WARNING, "In TapeDrivesCatalogueState::removeDrive(): Problem to remove tape drive from database.");
  }
}

void TapeDrivesCatalogueState::setDesiredDriveState(const std::string& drive,
  const common::dataStructures::DesiredDriveState & desiredState, log::LogContext &lc) {
  if (!desiredState.comment) {
    m_catalogue.DriveState()->setDesiredTapeDriveState(drive, desiredState);
  } else {
    m_catalogue.DriveState()->setDesiredTapeDriveStateComment(drive, desiredState.comment.value());
  }
}

void TapeDrivesCatalogueState::updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo,
  const ReportDriveStatsInputs& inputs, log::LogContext & lc) {
  common::dataStructures::TapeDriveStatistics statistics;
  statistics.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveInfo.host, inputs.reportTime);
  statistics.bytesTransferedInSession = inputs.bytesTransferred;
  statistics.filesTransferedInSession = inputs.filesTransferred;
  statistics.reportTime = inputs.reportTime;
  m_catalogue.DriveState()->updateTapeDriveStatistics(driveInfo.driveName, driveInfo.host, driveInfo.logicalLibrary,
                                                      statistics);
}

void TapeDrivesCatalogueState::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  cta::common::dataStructures::MountType mountType, common::dataStructures::DriveStatus status,
  time_t reportTime, log::LogContext& lc, uint64_t mountSessionId, uint64_t byteTransferred,
  uint64_t filesTransferred, const std::string& vid,
  const std::string& tapepool, const std::string& vo) {
  using common::dataStructures::DriveStatus;
  // Wrap all the parameters together for easier manipulation by sub-functions
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.byteTransferred = byteTransferred;
  inputs.filesTransferred = filesTransferred;
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
  common::dataStructures::TapeDrive driveState;
  // Set the parameters that we always set
  driveState.driveName = driveInfo.driveName;
  driveState.host = driveInfo.host;
  driveState.logicalLibrary = driveInfo.logicalLibrary;

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

  m_catalogue.DriveState()->updateTapeDriveStatus(driveState);
}

void TapeDrivesCatalogueState::setDriveDown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we are changing state, then all should be reset.
  driveState.sessionId = std::nullopt;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = inputs.reportTime;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = common::dataStructures::DriveStatus::Down;
  driveState.desiredUp = false;
  driveState.desiredForceDown = false;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = std::nullopt;
  if (inputs.reason) driveState.reasonUpDown = inputs.reason;
}

void TapeDrivesCatalogueState::setDriveUpOrMaybeDown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // Decide whether we should be up or down
  auto targetStatus = common::dataStructures::DriveStatus::Up;
  // If we are changing state, then all should be reset.
  driveState.sessionId = std::nullopt;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = inputs.reportTime;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = targetStatus;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = std::nullopt;
  if (inputs.reason) driveState.reasonUpDown = inputs.reason;
}

void TapeDrivesCatalogueState::setDriveProbing(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  using common::dataStructures::DriveStatus;
  // If we are changing state, then all should be reset.
  driveState.sessionId = std::nullopt;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = inputs.reportTime;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = inputs.status;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = std::nullopt;
}

void TapeDrivesCatalogueState::setDriveStarting(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we are changing state, then all should be reset.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = inputs.reportTime;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
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
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.mountStartTime = inputs.reportTime;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
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
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = inputs.byteTransferred;
  driveState.filesTransferedInSession = inputs.filesTransferred;
  driveState.sessionElapsedTime = 0;  // Because it just started
  driveState.transferStartTime = inputs.reportTime;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Transferring;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveUnloading(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = inputs.reportTime;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
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
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = inputs.reportTime;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
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
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = inputs.reportTime;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = std::nullopt;
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
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = inputs.reportTime;
  driveState.shutdownTime = std::nullopt;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::CleaningUp;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = (inputs.tapepool == "") ? std::nullopt : std::optional<std::string>(inputs.tapepool);
  driveState.currentActivity = std::nullopt;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveShutdown(common::dataStructures::TapeDrive & driveState,
  const ReportDriveStatusInputs & inputs) {
  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  driveState.sessionId = std::nullopt;
  driveState.bytesTransferedInSession = std::nullopt;
  driveState.filesTransferedInSession = std::nullopt;
  driveState.sessionStartTime = std::nullopt;
  driveState.sessionElapsedTime = std::nullopt;
  driveState.mountStartTime = std::nullopt;
  driveState.transferStartTime = std::nullopt;
  driveState.unloadStartTime = std::nullopt;
  driveState.unmountStartTime = std::nullopt;
  driveState.drainingStartTime = std::nullopt;
  driveState.downOrUpStartTime = std::nullopt;
  driveState.probeStartTime = std::nullopt;
  driveState.cleanupStartTime = std::nullopt;
  driveState.shutdownTime = inputs.reportTime;
  driveState.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Shutdown;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapepool;
  driveState.currentActivity = std::nullopt;
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
  tapeDriveStatus.downOrUpStartTime = reportTime;
  tapeDriveStatus.mountType = type;
  tapeDriveStatus.driveStatus = status;
  tapeDriveStatus.desiredUp = desiredState.up;
  tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  if (desiredState.reason) tapeDriveStatus.reasonUpDown = desiredState.reason;
  if (desiredState.comment) tapeDriveStatus.userComment = desiredState.comment;
  tapeDriveStatus.diskSystemName = std::nullopt;
  tapeDriveStatus.reservedBytes = std::nullopt;
  tapeDriveStatus.reservationSessionId = std::nullopt;
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
