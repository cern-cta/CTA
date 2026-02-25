/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/DriveStateCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

DriveStateCatalogueRetryWrapper::DriveStateCatalogueRetryWrapper(Catalogue& catalogue,
                                                                 log::Logger& log,
                                                                 const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void DriveStateCatalogueRetryWrapper::createTapeDrive(const common::dataStructures::TapeDrive& tapeDrive) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDrive] { return m_catalogue.DriveState()->createTapeDrive(tapeDrive); },
    m_maxTriesToConnect);
}

std::vector<std::string> DriveStateCatalogueRetryWrapper::getTapeDriveNames() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveState()->getTapeDriveNames(); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::TapeDrive> DriveStateCatalogueRetryWrapper::getTapeDrives() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveState()->getTapeDrives(); },
    m_maxTriesToConnect);
}

std::optional<common::dataStructures::TapeDrive>
DriveStateCatalogueRetryWrapper::getTapeDrive(const std::string& tapeDriveName) const {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName] { return m_catalogue.DriveState()->getTapeDrive(tapeDriveName); },
    m_maxTriesToConnect);
}

std::unordered_map<std::string, std::optional<uint64_t>> DriveStateCatalogueRetryWrapper::getTapeDriveMountIDs() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveState()->getTapeDriveMountIDs(); },
    m_maxTriesToConnect);
}

std::optional<common::dataStructures::DriveStatus>
DriveStateCatalogueRetryWrapper::getTapeDriveStatus(const std::string& tapeDriveName) const {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName] { return m_catalogue.DriveState()->getTapeDriveStatus(tapeDriveName); },
    m_maxTriesToConnect);
}

std::optional<common::dataStructures::MountType>
DriveStateCatalogueRetryWrapper::getMountType(const std::string& tapeDriveName) const {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName] { return m_catalogue.DriveState()->getMountType(tapeDriveName); },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::setDesiredTapeDriveState(
  const std::string& tapeDriveName,
  const common::dataStructures::DesiredDriveState& desiredState) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName, &desiredState] {
      return m_catalogue.DriveState()->setDesiredTapeDriveState(tapeDriveName, desiredState);
    },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
                                                                      const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName, &comment] {
      return m_catalogue.DriveState()->setDesiredTapeDriveStateComment(tapeDriveName, comment);
    },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::updateTapeDriveStatistics(
  const std::string& tapeDriveName,
  const std::string& host,
  const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName, &host, &logicalLibrary, &statistics] {
      return m_catalogue.DriveState()->updateTapeDriveStatistics(tapeDriveName, host, logicalLibrary, statistics);
    },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::updateTapeDriveStatus(const common::dataStructures::TapeDrive& tapeDrive) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDrive] { return m_catalogue.DriveState()->updateTapeDriveStatus(tapeDrive); },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::deleteTapeDrive(const std::string& tapeDriveName) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName] { return m_catalogue.DriveState()->deleteTapeDrive(tapeDriveName); },
    m_maxTriesToConnect);
}

std::map<std::string, uint64_t, std::less<>> DriveStateCatalogueRetryWrapper::getDiskSpaceReservations() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveState()->getDiskSpaceReservations(); },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::reserveDiskSpace(const std::string& driveName,
                                                       const uint64_t mountId,
                                                       const DiskSpaceReservationRequest& diskSpaceReservation,
                                                       log::LogContext& lc) {
  return retryOnLostConnection(
    m_log,
    [this, &driveName, &mountId, &diskSpaceReservation, &lc] {
      return m_catalogue.DriveState()->reserveDiskSpace(driveName, mountId, diskSpaceReservation, lc);
    },
    m_maxTriesToConnect);
}

void DriveStateCatalogueRetryWrapper::releaseDiskSpace(const std::string& driveName,
                                                       const uint64_t mountId,
                                                       const DiskSpaceReservationRequest& diskSpaceReservation,
                                                       log::LogContext& lc) {
  return retryOnLostConnection(
    m_log,
    [this, &driveName, &mountId, &diskSpaceReservation, &lc] {
      return m_catalogue.DriveState()->releaseDiskSpace(driveName, mountId, diskSpaceReservation, lc);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
