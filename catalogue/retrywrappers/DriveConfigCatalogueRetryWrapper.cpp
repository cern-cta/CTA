/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/DriveConfigCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

DriveConfigCatalogueRetryWrapper::DriveConfigCatalogueRetryWrapper(Catalogue& catalogue,
                                                                   log::Logger& log,
                                                                   const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void DriveConfigCatalogueRetryWrapper::createTapeDriveConfig(const std::string& driveName,
                                                             const std::string& category,
                                                             const std::string& keyName,
                                                             const std::string& value,
                                                             const std::string& source) {
  return retryOnLostConnection(
    m_log,
    [this, &driveName, &category, &keyName, &value, &source] {
      return m_catalogue.DriveConfig()->createTapeDriveConfig(driveName, category, keyName, value, source);
    },
    m_maxTriesToConnect);
}

std::vector<cta::catalogue::DriveConfigCatalogue::DriveConfig>
DriveConfigCatalogueRetryWrapper::getTapeDriveConfigs() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveConfig()->getTapeDriveConfigs(); },
    m_maxTriesToConnect);
}

std::vector<std::string>
DriveConfigCatalogueRetryWrapper::getTapeDriveNamesForSchedulerBackend(const std::string& schedulerBackendName) const {
  return retryOnLostConnection(
    m_log,
    [this, &schedulerBackendName] {
      return m_catalogue.DriveConfig()->getTapeDriveNamesForSchedulerBackend(schedulerBackendName);
    },
    m_maxTriesToConnect);
}

std::vector<std::pair<std::string, std::string>>
DriveConfigCatalogueRetryWrapper::getTapeDriveConfigNamesAndKeys() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DriveConfig()->getTapeDriveConfigNamesAndKeys(); },
    m_maxTriesToConnect);
}

void DriveConfigCatalogueRetryWrapper::modifyTapeDriveConfig(const std::string& driveName,
                                                             const std::string& category,
                                                             const std::string& keyName,
                                                             const std::string& value,
                                                             const std::string& source) {
  return retryOnLostConnection(
    m_log,
    [this, &driveName, &category, &keyName, &value, &source] {
      return m_catalogue.DriveConfig()->modifyTapeDriveConfig(driveName, category, keyName, value, source);
    },
    m_maxTriesToConnect);
}

std::optional<std::tuple<std::string, std::string, std::string>>
DriveConfigCatalogueRetryWrapper::getTapeDriveConfig(const std::string& tapeDriveName,
                                                     const std::string& keyName) const {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName, &keyName] { return m_catalogue.DriveConfig()->getTapeDriveConfig(tapeDriveName, keyName); },
    m_maxTriesToConnect);
}

void DriveConfigCatalogueRetryWrapper::deleteTapeDriveConfig(const std::string& tapeDriveName,
                                                             const std::string& keyName) {
  return retryOnLostConnection(
    m_log,
    [this, &tapeDriveName, &keyName] {
      return m_catalogue.DriveConfig()->deleteTapeDriveConfig(tapeDriveName, keyName);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
