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

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/DiskInstanceSpaceCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

DiskInstanceSpaceCatalogueRetryWrapper::DiskInstanceSpaceCatalogueRetryWrapper(
  const std::unique_ptr<Catalogue>& catalogue, log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void DiskInstanceSpaceCatalogueRetryWrapper::deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->deleteDiskInstanceSpace(name,
    diskInstance);},
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &diskInstance,
  const std::string &freeSpaceQueryURL,
  const uint64_t refreshInterval,
  const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(admin, name,
    diskInstance, freeSpaceQueryURL, refreshInterval, comment);},
    m_maxTriesToConnect);
}

std::list<common::dataStructures::DiskInstanceSpace> DiskInstanceSpaceCatalogueRetryWrapper::getAllDiskInstanceSpaces() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();},
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceComment(admin, name,
    diskInstance, comment);},
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceRefreshInterval(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const uint64_t refreshInterval) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(admin, name,
    diskInstance, refreshInterval);},
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceFreeSpace(const std::string &name,
  const std::string &diskInstance, const uint64_t freeSpace) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceFreeSpace(name,
    diskInstance, freeSpace);},
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceQueryURL(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &freeSpaceQueryURL) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(admin, name,
    diskInstance, freeSpaceQueryURL);},
    m_maxTriesToConnect);
}

} // namespace cta::catalogue
