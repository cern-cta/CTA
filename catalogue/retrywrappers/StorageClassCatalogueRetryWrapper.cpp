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

#include <list>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/StorageClassCatalogueRetryWrapper.hpp"
#include "common/dataStructures/StorageClass.hpp"

namespace cta {
namespace catalogue {

StorageClassCatalogueRetryWrapper::StorageClassCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                                                     log::Logger& log,
                                                                     const uint32_t maxTriesToConnect) :
m_catalogue(catalogue),
m_log(log),
m_maxTriesToConnect(maxTriesToConnect) {}

void StorageClassCatalogueRetryWrapper::createStorageClass(const common::dataStructures::SecurityIdentity& admin,
                                                           const common::dataStructures::StorageClass& storageClass) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->createStorageClass(admin, storageClass); }, m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::deleteStorageClass(const std::string& storageClassName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->deleteStorageClass(storageClassName); }, m_maxTriesToConnect);
}

std::list<common::dataStructures::StorageClass> StorageClassCatalogueRetryWrapper::getStorageClasses() const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->getStorageClasses(); }, m_maxTriesToConnect);
}

common::dataStructures::StorageClass StorageClassCatalogueRetryWrapper::getStorageClass(const std::string& name) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->getStorageClass(name); }, m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassNbCopies(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t nbCopies) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->modifyStorageClassNbCopies(admin, name, nbCopies); },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin,
                                                                  const std::string& name,
                                                                  const std::string& comment) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->modifyStorageClassComment(admin, name, comment); },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassVo(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& name,
                                                             const std::string& vo) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->modifyStorageClassVo(admin, name, vo); }, m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin,
                                                               const std::string& currentName,
                                                               const std::string& newName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->StorageClass()->modifyStorageClassName(admin, currentName, newName); },
    m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta
