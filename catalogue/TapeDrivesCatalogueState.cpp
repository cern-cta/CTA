/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeDrivesCatalogueState.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "telemetry/metrics/DriveState.hpp"
#include "version.hpp"

#include <algorithm>
#include <chrono>
#include <list>

namespace cta {

struct TapeDrivesCatalogueState::ReportDriveStatusInputs {
  common::dataStructures::DriveStatus status;
  cta::common::dataStructures::MountType mountType;
  time_t reportTime;
  uint64_t mountSessionId;
  uint64_t bytesTransferred;
  uint64_t filesTransferred;
  std::string vid;
  std::string tapePool;
  std::string vo;
  uint64_t priority = 0;
  std::optional<std::string> activity;
};

TapeDrivesCatalogueState::TapeDrivesCatalogueState(catalogue::Catalogue& catalogue) : m_catalogue(catalogue) {}

std::optional<common::dataStructures::DriveStatus>
TapeDrivesCatalogueState::catalogueStatus(const common::dataStructures::DriveReport& report) {
  using common::dataStructures::DriveStatus;
  using common::dataStructures::ReportedDriveState;
  if (report.state == ReportedDriveState::Down && report.tapeSession) {
    throw exception::Exception("Cannot report Down with tape activity: hardware ownership must have ended");
  }
  if (!report.tapeSession) {
    switch (report.state) {
      case ReportedDriveState::Down:
        return DriveStatus::Down;
      case ReportedDriveState::Preparing:
        return DriveStatus::Starting;
      case ReportedDriveState::Up:
        return DriveStatus::Up;
    }
  }

  // The schema stores only one status: retain activity until the owner reports no active session.
  using enum common::dataStructures::TapeSessionState;
  switch (report.tapeSession->state) {
    case Preparing:
      return DriveStatus::Starting;
    case Mounting:
      return DriveStatus::Mounting;
    case Transferring:
      return DriveStatus::Transferring;
    case Unloading:
      return DriveStatus::Unloading;
    case Unmounting:
      return DriveStatus::Unmounting;
    case DrainingToDisk:
      return DriveStatus::DrainingToDisk;
    case Loading:
    case Finalizing:
    case Finished:
      return std::nullopt;
  }
  return std::nullopt;
}

void TapeDrivesCatalogueState::reportDriveState(const common::dataStructures::DriveInfo& driveInfo,
                                                const common::dataStructures::DriveReport& report,
                                                log::LogContext& lc) const {
  const auto status = catalogueStatus(report);
  if (!status) {
    return;
  }

  ReportDriveStatusInputs inputs {};
  inputs.status = *status;
  inputs.mountType = common::dataStructures::MountType::NoMount;
  inputs.reportTime = report.reportTime;
  if (report.tapeSession) {
    const auto& session = *report.tapeSession;
    inputs.mountType = session.mountType;
    inputs.mountSessionId = session.mountSessionId;
    inputs.bytesTransferred = session.bytesTransferred;
    inputs.filesTransferred = session.filesTransferred;
    inputs.vid = session.vid;
    inputs.tapePool = session.tapePool;
    inputs.vo = session.vo;
    inputs.priority = session.priority;
    inputs.activity = session.activity;
  }
  updateDriveStatus(driveInfo, inputs, lc);
}

void TapeDrivesCatalogueState::createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                                     const common::dataStructures::DesiredDriveState& desiredState,
                                                     const common::dataStructures::MountType& type,
                                                     const common::dataStructures::DriveStatus& status,
                                                     const common::dataStructures::SecurityIdentity& identity,
                                                     log::LogContext& lc) const {
  telemetry::metrics::setDriveStatus(status);
  auto tapeDriveStatus = setTapeDriveStatus(driveInfo, desiredState, type, status, identity);
  auto driveNames = m_catalogue.DriveState()->getTapeDriveNames();
  if (auto it = std::find(driveNames.begin(), driveNames.end(), tapeDriveStatus.driveName); it != driveNames.end()) {
    m_catalogue.DriveState()->deleteTapeDrive(tapeDriveStatus.driveName);
  }
  tapeDriveStatus.ctaVersion = CTA_VERSION;
  m_catalogue.DriveState()->createTapeDrive(tapeDriveStatus);
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveInfo.driveName);
  lc.log(log::DEBUG, "In TapeDrivesCatalogueState::createTapeDriveStatus(): success.");
}

void TapeDrivesCatalogueState::checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo& driveInfo) const {
  const auto tapeDrive = m_catalogue.DriveState()->getTapeDrive(driveInfo.driveName);
  if (!tapeDrive) {
    return;
  }
  if (tapeDrive.value().logicalLibrary != driveInfo.logicalLibrary || tapeDrive.value().host != driveInfo.host) {
    throw DriveAlreadyExistsException(
      std::string("The drive name=") + driveInfo.driveName + " logicalLibrary=" + driveInfo.logicalLibrary
      + " host=" + driveInfo.host + " cannot be created because a drive with a same name with logicalLibrary="
      + tapeDrive.value().logicalLibrary + " host=" + tapeDrive.value().host + " already exists.");
  }
}

void TapeDrivesCatalogueState::removeDrive(const std::string& drive, log::LogContext& lc) const {
  try {
    m_catalogue.DriveState()->deleteTapeDrive(drive);
    log::ScopedParamContainer params(lc);
    params.add("driveName", drive);
    lc.log(log::INFO, "In TapeDrivesCatalogueState::removeDrive(): removed tape drive from database.");
  } catch (cta::exception::Exception&) {
    lc.log(log::WARNING, "In TapeDrivesCatalogueState::removeDrive(): Problem to remove tape drive from database.");
  }
}

common::dataStructures::DesiredDriveState
TapeDrivesCatalogueState::getDesiredDriveState(const std::string& driveName) const {
  const auto optionalDriveState = m_catalogue.DriveState()->getTapeDrive(driveName);
  if (!optionalDriveState.has_value()) {
    throw TapeDrivesCatalogueState::NoSuchDrive("In TapeDrivesCatalogueState::getDesiredDriveState(): no such drive: "
                                                + driveName);
  }
  const auto& driveState = optionalDriveState.value();

  common::dataStructures::DesiredDriveState desiredDriveState;
  desiredDriveState.up = driveState.desiredUp;
  desiredDriveState.forceDown = driveState.desiredForceDown;
  desiredDriveState.reason = driveState.reasonUpDown;
  desiredDriveState.comment = driveState.userComment;
  return desiredDriveState;
}

void TapeDrivesCatalogueState::writeDesiredState(const std::string& drive,
                                                 const common::dataStructures::DesiredDriveState& desiredState,
                                                 log::LogContext& lc,
                                                 bool logStateChange) const {
  log::ScopedParamContainer spc(lc);
  spc.add("tapeDrive", drive)
    .add("drive_name", drive)  // Compatibility with the tapealerting drive-down hook.
    .add("up", desiredState.up ? "up" : "down")
    .add("force", desiredState.forceDown ? "yes" : "no")
    .add("comment", desiredState.comment.value_or(""));
  if (desiredState.reason) {
    spc.add("reason", *desiredState.reason);
  } else {
    spc.add("reasonPreserved", true);
  }
  if (logStateChange) {
    lc.logEvent(log::INFO,
                desiredState.up ? "Putting the drive up" : "Putting the drive down",
                desiredState.up ? semconv::log::EventNameValues::kPuttingTapeDriveUp :
                                  semconv::log::EventNameValues::kPuttingTapeDriveDown);
  } else {
    lc.log(log::DEBUG, "Updating desired state of an already Down drive.");
  }
  m_catalogue.DriveState()->setDesiredTapeDriveState(drive, desiredState);
}

void TapeDrivesCatalogueState::requestDriveUp(const std::string& driveName,
                                              std::string_view reason,
                                              log::LogContext& lc) const {
  common::dataStructures::DesiredDriveState desired;
  desired.up = true;
  desired.reason = reason;
  writeDesiredState(driveName, desired, lc);
}

void TapeDrivesCatalogueState::requestDriveDown(const std::string& driveName,
                                                const common::dataStructures::DriveDownRequest& request,
                                                log::LogContext& lc) const {
  using namespace common::dataStructures;
  DesiredDriveState desired;
  desired.forceDown = request.force;
  desired.reason = request.reason;
  const auto current = m_catalogue.DriveState()->getTapeDrive(driveName);
  if (!current) {
    throw NoSuchDrive("Cannot request Down for missing drive: " + driveName);
  }
  if (request.reasonPolicy == ExistingReasonPolicy::PreserveSpecific) {
    if (!current->desiredUp && current->reasonUpDown && !current->reasonUpDown->empty()
        && classifyPersistedDriveReason(*current->reasonUpDown) == PersistedDriveReasonKind::Other) {
      // Omit the reason rather than writing a stale snapshot over a concurrent operator update.
      desired.reason.reset();
    }
  }
  // Still apply reason and force changes when Down; only the transition event is suppressed.
  writeDesiredState(driveName, desired, lc, current->desiredUp);
}

void TapeDrivesCatalogueState::setDriveComment(const std::string& driveName,
                                               std::string_view comment,
                                               log::LogContext& lc) const {
  m_catalogue.DriveState()->setDesiredTapeDriveStateComment(driveName, std::string(comment));
  log::ScopedParamContainer params(lc);
  params.add("tapeDrive", driveName).add("comment", comment);
  lc.log(log::INFO, "Updated drive comment.");
}

void TapeDrivesCatalogueState::updateDriveStatistics(const common::dataStructures::DriveInfo& driveInfo,
                                                     const ReportDriveStatsInputs& inputs,
                                                     [[maybe_unused]] log::LogContext& lc) const {
  common::dataStructures::TapeDriveStatistics statistics;
  statistics.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  statistics.bytesTransferedInSession = inputs.bytesTransferred;
  statistics.filesTransferedInSession = inputs.filesTransferred;
  statistics.reportTime = inputs.reportTime;
  m_catalogue.DriveState()->updateTapeDriveStatistics(driveInfo.driveName,
                                                      driveInfo.host,
                                                      driveInfo.logicalLibrary,
                                                      statistics);
}

void TapeDrivesCatalogueState::updateDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                                 const ReportDriveStatusInputs& inputs,
                                                 log::LogContext& lc) const {
  telemetry::metrics::setDriveStatus(inputs.status);
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
      setDriveUp(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Starting:
      setDriveStarting(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Mounting:
      setDriveMounting(driveState, inputs);
      break;
    case common::dataStructures::DriveStatus::Transferring:
      setDriveTransferring(driveState, inputs);
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
    default:
      throw exception::Exception("Unexpected legacy status in TapeDrivesCatalogueState::updateDriveStatus");
  }

  driveState.currentPriority = driveState.mountType == common::dataStructures::MountType::NoMount ? 0 : inputs.priority;
  if (inputs.activity) {
    driveState.currentActivity = inputs.activity;
  }

  const bool statusChanged = m_catalogue.DriveState()->updateTapeDriveStatus(driveState);
  if (statusChanged) {
    log::ScopedParamContainer params(lc);
    params.add("next_status", common::dataStructures::TapeDrive::stateToString(inputs.status));
    lc.log(log::INFO, "Drive status updated.");

    // Keep the full update payload available for diagnostics.
    driveState.convertToLogParams(params, "update_");
    lc.log(log::DEBUG, "Drive status update details.");
  }
}

void TapeDrivesCatalogueState::setDriveDown(common::dataStructures::TapeDrive& driveState,
                                            const ReportDriveStatusInputs& inputs) const {
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
  driveState.nextMountType = common::dataStructures::MountType::NoMount;
  driveState.driveStatus = common::dataStructures::DriveStatus::Down;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = "";
}

void TapeDrivesCatalogueState::setDriveUp(common::dataStructures::TapeDrive& driveState,
                                          const ReportDriveStatusInputs& inputs) const {
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
  driveState.driveStatus = common::dataStructures::DriveStatus::Up;
  driveState.currentVid = "";
  driveState.currentTapePool = "";
  driveState.currentVo = "";
  driveState.currentActivity = "";
}

void TapeDrivesCatalogueState::setDriveStarting(common::dataStructures::TapeDrive& driveState,
                                                const ReportDriveStatusInputs& inputs) const {
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
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Starting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
  driveState.currentActivity = inputs.activity;
}

void TapeDrivesCatalogueState::setDriveMounting(common::dataStructures::TapeDrive& driveState,
                                                const ReportDriveStatusInputs& inputs) const {
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
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Mounting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveTransferring(common::dataStructures::TapeDrive& driveState,
                                                    const ReportDriveStatusInputs& inputs) const {
  driveState.sessionId = inputs.mountSessionId;
  driveState.bytesTransferedInSession = inputs.bytesTransferred;
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
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveUnloading(common::dataStructures::TapeDrive& driveState,
                                                 const ReportDriveStatusInputs& inputs) const {
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
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Unloading;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveUnmounting(common::dataStructures::TapeDrive& driveState,
                                                  const ReportDriveStatusInputs& inputs) const {
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
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::Unmounting;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
}

void TapeDrivesCatalogueState::setDriveDrainingToDisk(common::dataStructures::TapeDrive& driveState,
                                                      const ReportDriveStatusInputs& inputs) const {
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
  driveState.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveState.host, inputs.reportTime);
  driveState.mountType = inputs.mountType;
  driveState.driveStatus = common::dataStructures::DriveStatus::DrainingToDisk;
  driveState.currentVid = inputs.vid;
  driveState.currentTapePool = inputs.tapePool;
  driveState.currentVo = inputs.vo;
}

common::dataStructures::TapeDrive
TapeDrivesCatalogueState::setTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                                             const common::dataStructures::DesiredDriveState& desiredState,
                                             const common::dataStructures::MountType& type,
                                             const common::dataStructures::DriveStatus& status,
                                             const common::dataStructures::SecurityIdentity& identity) const {
  const time_t reportTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  common::dataStructures::TapeDrive tapeDriveStatus;
  tapeDriveStatus.driveName = driveInfo.driveName;
  tapeDriveStatus.host = driveInfo.host;
  tapeDriveStatus.logicalLibrary = driveInfo.logicalLibrary;
  tapeDriveStatus.downOrUpStartTime = reportTime;
  tapeDriveStatus.mountType = type;
  tapeDriveStatus.driveStatus = status;
  tapeDriveStatus.desiredUp = desiredState.up;
  tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  if (desiredState.reason) {
    tapeDriveStatus.reasonUpDown = desiredState.reason;
  }
  if (desiredState.comment) {
    tapeDriveStatus.userComment = desiredState.comment;
  }
  tapeDriveStatus.diskSystemName = std::nullopt;
  tapeDriveStatus.reservedBytes = std::nullopt;
  tapeDriveStatus.reservationSessionId = std::nullopt;
  tapeDriveStatus.devFileName = driveInfo.devFilename;
  tapeDriveStatus.rawLibrarySlot = driveInfo.rawLibrarySlot;
  if (identity.username.empty()) {
    tapeDriveStatus.creationLog = common::dataStructures::EntryLog("NO_USER", driveInfo.host, reportTime);
    tapeDriveStatus.lastModificationLog = common::dataStructures::EntryLog("NO_USER", driveInfo.host, reportTime);
  } else {
    tapeDriveStatus.creationLog = common::dataStructures::EntryLog(identity.username, identity.host, reportTime);
    tapeDriveStatus.lastModificationLog =
      common::dataStructures::EntryLog(identity.username, identity.host, reportTime);
  }
  return tapeDriveStatus;
}

}  // namespace cta
