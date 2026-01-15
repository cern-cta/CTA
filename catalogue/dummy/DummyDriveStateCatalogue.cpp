/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyDriveStateCatalogue.hpp"

#include "catalogue/CatalogueExceptions.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

void DummyDriveStateCatalogue::createTapeDrive(const common::dataStructures::TapeDrive& tapeDrive) {
  throw exception::NotImplementedException();
}

void DummyDriveStateCatalogue::deleteTapeDrive(const std::string& tapeDriveName) {
  throw exception::NotImplementedException();
}

std::vector<std::string> DummyDriveStateCatalogue::getTapeDriveNames() const {
  return {m_tapeDriveStatus.driveName};
}

std::optional<common::dataStructures::TapeDrive>
DummyDriveStateCatalogue::getTapeDrive(const std::string& tapeDriveName) const {
  if (m_tapeDriveStatus.driveName != "") {
    return m_tapeDriveStatus;
  }
  common::dataStructures::TapeDrive tapeDriveStatus;
  const time_t reportTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  tapeDriveStatus.driveName = tapeDriveName;
  tapeDriveStatus.host = "Dummy_Host";
  tapeDriveStatus.logicalLibrary = "Dummy_Library";

  tapeDriveStatus.downOrUpStartTime = reportTime;

  tapeDriveStatus.mountType = common::dataStructures::MountType::NoMount;
  tapeDriveStatus.driveStatus = common::dataStructures::DriveStatus::Down;
  tapeDriveStatus.desiredUp = false;
  tapeDriveStatus.desiredForceDown = false;

  tapeDriveStatus.diskSystemName = "Dummy_System";
  tapeDriveStatus.reservedBytes = 0;
  tapeDriveStatus.reservationSessionId = 0;

  return tapeDriveStatus;
}

std::vector<common::dataStructures::TapeDrive> DummyDriveStateCatalogue::getTapeDrives() const {
  std::vector<common::dataStructures::TapeDrive> tapeDrives;
  if (const auto tapeDrive = getTapeDrive(m_tapeDriveStatus.driveName); tapeDrive.has_value()) {
    tapeDrives.push_back(tapeDrive.value());
  }
  return tapeDrives;
}

std::unordered_map<std::string, std::optional<uint64_t>> DummyDriveStateCatalogue::getTapeDriveMountIDs() const {
  std::vector<common::dataStructures::TapeDrive> tapeDrives = getTapeDrives();
  std::unordered_map<std::string, std::optional<uint64_t>> tapeDriveMountIDs;
  for (auto drive : tapeDrives) {
    tapeDriveMountIDs[drive.driveName] = drive.sessionId;
  }
  return tapeDriveMountIDs;
}

void DummyDriveStateCatalogue::setDesiredTapeDriveState(const std::string&,
                                                        const common::dataStructures::DesiredDriveState& desiredState) {
  m_tapeDriveStatus.desiredUp = desiredState.up;
  m_tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  m_tapeDriveStatus.reasonUpDown = desiredState.reason;
}

void DummyDriveStateCatalogue::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
                                                               const std::string& comment) {
  m_tapeDriveStatus.userComment = comment;
}

void DummyDriveStateCatalogue::updateTapeDriveStatistics(
  const std::string& tapeDriveName,
  const std::string& host,
  const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  m_tapeDriveStatus.driveName = tapeDriveName;
  m_tapeDriveStatus.host = host;
  m_tapeDriveStatus.logicalLibrary = logicalLibrary;
  m_tapeDriveStatus.bytesTransferedInSession = statistics.bytesTransferedInSession;
  m_tapeDriveStatus.filesTransferedInSession = statistics.filesTransferedInSession;
  m_tapeDriveStatus.lastModificationLog = statistics.lastModificationLog;
}

void DummyDriveStateCatalogue::updateTapeDriveStatus(const common::dataStructures::TapeDrive& tapeDrive) {
  m_tapeDriveStatus = tapeDrive;
}

std::map<std::string, uint64_t, std::less<>> DummyDriveStateCatalogue::getDiskSpaceReservations() const {
  std::map<std::string, uint64_t, std::less<>> ret;
  const auto tdNames = getTapeDriveNames();
  for (const auto& driveName : tdNames) {
    const auto tdStatus = getTapeDrive(driveName);
    if (tdStatus.value().diskSystemName) {
      // no need to check key, operator[] initializes missing values at zero for scalar types
      ret[tdStatus.value().diskSystemName.value()] += tdStatus.value().reservedBytes.value();
    }
  }
  return ret;
}

void DummyDriveStateCatalogue::reserveDiskSpace(const std::string& driveName,
                                                const uint64_t mountId,
                                                const DiskSpaceReservationRequest& diskSpaceReservation,
                                                log::LogContext& lc) {
  if (diskSpaceReservation.empty()) {
    return;
  }

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
    .add("diskSystem", diskSpaceReservation.begin()->first)
    .add("reservationBytes", diskSpaceReservation.begin()->second)
    .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request.");

  auto tdStatus = getTapeDrive(driveName);
  if (!tdStatus.has_value()) {
    return;
  }

  if (!tdStatus.value().reservationSessionId) {
    tdStatus.value().reservationSessionId = mountId;
    tdStatus.value().reservedBytes = 0;
  }

  if (tdStatus.value().reservationSessionId != mountId) {
    tdStatus.value().reservationSessionId = mountId;
    tdStatus.value().reservedBytes = 0;
  }

  tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
  tdStatus.value().reservedBytes.value() += diskSpaceReservation.begin()->second;
  updateTapeDriveStatus(tdStatus.value());
}

void DummyDriveStateCatalogue::releaseDiskSpace(const std::string& driveName,
                                                const uint64_t mountId,
                                                const DiskSpaceReservationRequest& diskSpaceReservation,
                                                log::LogContext& lc) {
  if (diskSpaceReservation.empty()) {
    return;
  }

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
    .add("diskSystem", diskSpaceReservation.begin()->first)
    .add("reservationBytes", diskSpaceReservation.begin()->second)
    .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");

  auto tdStatus = getTapeDrive(driveName);

  if (!tdStatus.has_value()) {
    return;
  }
  if (!tdStatus.value().reservationSessionId) {
    return;
  }
  if (tdStatus.value().reservationSessionId != mountId) {
    return;
  }
  auto& bytes = diskSpaceReservation.begin()->second;
  if (bytes > tdStatus.value().reservedBytes) {
    throw exception::NegativeDiskSpaceReservationReached(
      "In DriveState::subtractDiskSpaceReservation(): we would reach a negative reservation size.");
  }
  tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
  tdStatus.value().reservedBytes.value() -= bytes;
  updateTapeDriveStatus(tdStatus.value());
}

}  // namespace cta::catalogue
