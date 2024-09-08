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
#include "catalogue/retrywrappers/ArchiveRouteCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

namespace cta::catalogue {

ArchiveRouteCatalogueRetryWrapper::ArchiveRouteCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void ArchiveRouteCatalogueRetryWrapper::createArchiveRoute(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
  const std::string &tapePoolName, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&storageClassName,&copyNb,&archiveRouteType,&tapePoolName,&comment] {
    return m_catalogue->ArchiveRoute()->createArchiveRoute(admin, storageClassName, copyNb, archiveRouteType, tapePoolName, comment);
  }, m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType) {
  return retryOnLostConnection(m_log, [this,&storageClassName,&copyNb,&archiveRouteType] {
    return m_catalogue->ArchiveRoute()->deleteArchiveRoute(storageClassName, copyNb, archiveRouteType);
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveRoute> ArchiveRouteCatalogueRetryWrapper::getArchiveRoutes() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->ArchiveRoute()->getArchiveRoutes();
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveRoute> ArchiveRouteCatalogueRetryWrapper::getArchiveRoutes(
  const std::string &storageClassName, const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [this,&storageClassName,&tapePoolName] {
    return m_catalogue->ArchiveRoute()->getArchiveRoutes(storageClassName, tapePoolName);
  }, m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::modifyArchiveRouteTapePoolName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb,
  const common::dataStructures::ArchiveRouteType &archiveRouteType, const std::string &tapePoolName) {
  return retryOnLostConnection(m_log, [this,&admin,&storageClassName,&copyNb,&archiveRouteType,&tapePoolName] {
    return m_catalogue->ArchiveRoute()->modifyArchiveRouteTapePoolName(admin, storageClassName, copyNb, archiveRouteType, tapePoolName);
  }, m_maxTriesToConnect);
}

void ArchiveRouteCatalogueRetryWrapper::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&storageClassName,&copyNb,&archiveRouteType,&comment] {
    return m_catalogue->ArchiveRoute()->modifyArchiveRouteComment(admin, storageClassName, copyNb, archiveRouteType, comment);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
