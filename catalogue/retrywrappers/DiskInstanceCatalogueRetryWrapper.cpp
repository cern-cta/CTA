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
#include "catalogue/retrywrappers/DiskInstanceCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"

namespace cta::catalogue {

DiskInstanceCatalogueRetryWrapper::DiskInstanceCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger& log, const uint32_t maxTriesToConnect) :
  m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void DiskInstanceCatalogueRetryWrapper::createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
  const std::string& name, const std::string& comment) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&comment] {
      return m_catalogue->DiskInstance()->createDiskInstance(admin, name, comment);
    }, m_maxTriesToConnect);
}

void DiskInstanceCatalogueRetryWrapper::deleteDiskInstance(const std::string& name) {
  return retryOnLostConnection(m_log, [this,&name] {
      return m_catalogue->DiskInstance()->deleteDiskInstance(name);
    }, m_maxTriesToConnect);
}

void DiskInstanceCatalogueRetryWrapper::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
  const std::string& name, const std::string& comment) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&comment] {
      return m_catalogue->DiskInstance()->modifyDiskInstanceComment(admin, name, comment);
    }, m_maxTriesToConnect);
}

std::list<common::dataStructures::DiskInstance> DiskInstanceCatalogueRetryWrapper::getAllDiskInstances() const {
  return retryOnLostConnection(m_log, [this] {
      return m_catalogue->DiskInstance()->getAllDiskInstances();
    }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
