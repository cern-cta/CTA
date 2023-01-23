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

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/DriveConfigCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/log/Logger.hpp"

namespace cta {
namespace catalogue {

DriveConfigCatalogueRetryWrapper::DriveConfigCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void DriveConfigCatalogueRetryWrapper::createTapeDriveConfig(const std::string &driveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->createTapeDriveConfig(driveName, category,
    keyName, value, source);},
    m_maxTriesToConnect);
}

std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> DriveConfigCatalogueRetryWrapper::getTapeDriveConfigs() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->getTapeDriveConfigs();},
    m_maxTriesToConnect);
}

std::list<std::pair<std::string, std::string>> DriveConfigCatalogueRetryWrapper::getTapeDriveConfigNamesAndKeys() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->getTapeDriveConfigNamesAndKeys();},
    m_maxTriesToConnect);
}

void DriveConfigCatalogueRetryWrapper::modifyTapeDriveConfig(const std::string &driveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->modifyTapeDriveConfig(driveName, category,
    keyName, value, source);},
    m_maxTriesToConnect);
}

std::optional<std::tuple<std::string, std::string, std::string>> DriveConfigCatalogueRetryWrapper::getTapeDriveConfig(
  const std::string &tapeDriveName,
  const std::string &keyName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, keyName);},
    m_maxTriesToConnect);
}

void DriveConfigCatalogueRetryWrapper::deleteTapeDriveConfig(const std::string &tapeDriveName,
  const std::string &keyName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName,
    keyName);},
    m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta
