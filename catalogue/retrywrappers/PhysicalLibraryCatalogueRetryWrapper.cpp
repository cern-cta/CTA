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

namespace cta {
namespace catalogue {

PhysicalLibraryCatalogueRetryWrapper::PhysicalLibraryCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void PhysicalLibraryCatalogueRetryWrapper::createPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
 const common::dataStructures::PhysicalLibrary& pl) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->createPhysicalLibrary(admin, pl);},
   m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::deletePhysicalLibrary(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->deletePhysicalLibrary(name);},
    m_maxTriesToConnect);
}

std::list<common::dataStructures::PhysicalLibrary> PhysicalLibraryCatalogueRetryWrapper::getPhysicalLibraries() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->getPhysicalLibraries();},
    m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryName(admin,
    currentName, newName);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryGuiUrl(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &guiUrl) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryGuiUrl(admin,
    name, guiUrl);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryWebcamUrl(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &webcamUrl) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryWebcamUrl(admin,
    name, webcamUrl);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryLocation(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &location) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryLocation(admin,
    name, location);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryNbPhysicalCartridgeSlots(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t &nbPhysicalCartridgeSlots) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryNbPhysicalCartridgeSlots(admin,
    name, nbPhysicalCartridgeSlots);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryNbAvailableCartridgeSlots(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t &nbAvailableCartridgeSlots) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryNbAvailableCartridgeSlots(admin,
    name, nbAvailableCartridgeSlots);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryNbPhysicalDriveSlots(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const uint64_t &nbPhysicalDriveSlots) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryNbPhysicalDriveSlots(admin,
    currentName, nbPhysicalDriveSlots);}, m_maxTriesToConnect);
}

void PhysicalLibraryCatalogueRetryWrapper::modifyPhysicalLibraryComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->PhysicalLibrary()->modifyPhysicalLibraryComment(admin,
    name, comment);}, m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta