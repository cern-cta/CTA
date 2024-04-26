/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "catalogue/CatalogueExceptions.hpp"
#include "catalogue/dummy/DummyDriveStateCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

void DummyDriveStateCatalogue::createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyDriveStateCatalogue::deleteTapeDrive(const std::string &tapeDriveName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<std::string> DummyDriveStateCatalogue::getTapeDriveNames() const {
  return {m_tapeDriveStatus.driveName};
}

std::optional<common::dataStructures::TapeDrive> DummyDriveStateCatalogue::getTapeDrive(
  const std::string &tapeDriveName) const {
  if (m_tapeDriveStatus.driveName != "") return m_tapeDriveStatus;
  common::dataStructures::TapeDrive tapeDriveStatus;
  const time_t reportTime = time(nullptr);

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

std::list<common::dataStructures::TapeDrive> DummyDriveStateCatalogue::getTapeDrives() const {
  std::list<common::dataStructures::TapeDrive> tapeDrives;
  if (const auto tapeDrive = getTapeDrive(m_tapeDriveStatus.driveName); tapeDrive.has_value()) {
    tapeDrives.push_back(tapeDrive.value());
  }
  return tapeDrives;
}

void DummyDriveStateCatalogue::setDesiredTapeDriveState(const std::string&,
    const common::dataStructures::DesiredDriveState &desiredState) {
  m_tapeDriveStatus.desiredUp = desiredState.up;
  m_tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  m_tapeDriveStatus.reasonUpDown = desiredState.reason;
}

void DummyDriveStateCatalogue::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
  const std::string &comment) {
  m_tapeDriveStatus.userComment = comment;
}

void DummyDriveStateCatalogue::updateTapeDriveStatistics(const std::string& tapeDriveName,
  const std::string& host, const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  m_tapeDriveStatus.driveName = tapeDriveName;
  m_tapeDriveStatus.host = host;
  m_tapeDriveStatus.logicalLibrary = logicalLibrary;
  m_tapeDriveStatus.bytesTransferedInSession = statistics.bytesTransferedInSession;
  m_tapeDriveStatus.filesTransferedInSession = statistics.filesTransferedInSession;
  m_tapeDriveStatus.lastModificationLog = statistics.lastModificationLog;
}

void DummyDriveStateCatalogue::updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
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

void DummyDriveStateCatalogue::reserveDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
        .add("diskSystem", diskSpaceReservation.begin()->first)
        .add("reservationBytes", diskSpaceReservation.begin()->second)
        .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request.");

  auto tdStatus = getTapeDrive(driveName);
  if (!tdStatus) return;

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

void DummyDriveStateCatalogue::releaseDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
        .add("diskSystem", diskSpaceReservation.begin()->first)
        .add("reservationBytes", diskSpaceReservation.begin()->second)
        .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");

  auto tdStatus = getTapeDrive(driveName);

  if (!tdStatus) return;
  if (!tdStatus.value().reservationSessionId) {
    return;
  }
  if (tdStatus.value().reservationSessionId != mountId) {
    return;
  }
  auto& bytes = diskSpaceReservation.begin()->second;
  if (bytes > tdStatus.value().reservedBytes) throw exception::NegativeDiskSpaceReservationReached(
    "In DriveState::subtractDiskSpaceReservation(): we would reach a negative reservation size.");
  tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
  tdStatus.value().reservedBytes.value() -= bytes;
  updateTapeDriveStatus(tdStatus.value());
}

} // namespace cta::catalogue
