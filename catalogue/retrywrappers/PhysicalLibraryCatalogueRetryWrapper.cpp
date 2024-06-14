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
#include "catalogue/retrywrappers/PhysicalLibraryCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

namespace cta::catalogue {

PhysicalLibraryCatalogueRetryWrapper::PhysicalLibraryCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void PhysicalLibraryCatalogueRetryWrapper::createPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
 const common::dataStructures::PhysicalLibrary& pl) {
  return retryOnLostConnection(m_log, [this,&admin,&pl] {
    return m_catalogue->PhysicalLibrary()->createPhysicalLibrary(admin, pl);
  }, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::deletePhysicalLibrary(const std::string &name) {
  return retryOnLostConnection(m_log, [this,&name] {
    return m_catalogue->PhysicalLibrary()->deletePhysicalLibrary(name);
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::PhysicalLibrary> PhysicalLibraryCatalogueRetryWrapper::getPhysicalLibraries() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
  }, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibrary(
  const common::dataStructures::SecurityIdentity &admin,  common::dataStructures::UpdatePhysicalLibrary& pl) {
  return retryOnLostConnection(m_log, [this,&admin,&pl] {
    return m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(admin, pl);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
