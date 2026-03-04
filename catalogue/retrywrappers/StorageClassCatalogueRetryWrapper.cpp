/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/StorageClassCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/StorageClass.hpp"

#include <list>
#include <string>

namespace cta::catalogue {

StorageClassCatalogueRetryWrapper::StorageClassCatalogueRetryWrapper(Catalogue& catalogue,
                                                                     log::Logger& log,
                                                                     const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void StorageClassCatalogueRetryWrapper::createStorageClass(const common::dataStructures::SecurityIdentity& admin,
                                                           const common::dataStructures::StorageClass& storageClass) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &storageClass] { return m_catalogue.StorageClass()->createStorageClass(admin, storageClass); },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::deleteStorageClass(const std::string& storageClassName) {
  return retryOnLostConnection(
    m_log,
    [this, &storageClassName] { return m_catalogue.StorageClass()->deleteStorageClass(storageClassName); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::StorageClass> StorageClassCatalogueRetryWrapper::getStorageClasses() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.StorageClass()->getStorageClasses(); },
    m_maxTriesToConnect);
}

common::dataStructures::StorageClass StorageClassCatalogueRetryWrapper::getStorageClass(const std::string& name) const {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.StorageClass()->getStorageClass(name); },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassNbCopies(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t nbCopies) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &nbCopies] {
      return m_catalogue.StorageClass()->modifyStorageClassNbCopies(admin, name, nbCopies);
    },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin,
                                                                  const std::string& name,
                                                                  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &comment] {
      return m_catalogue.StorageClass()->modifyStorageClassComment(admin, name, comment);
    },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassVo(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& name,
                                                             const std::string& vo) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &vo] { return m_catalogue.StorageClass()->modifyStorageClassVo(admin, name, vo); },
    m_maxTriesToConnect);
}

void StorageClassCatalogueRetryWrapper::modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin,
                                                               const std::string& currentName,
                                                               const std::string& newName) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &currentName, &newName] {
      return m_catalogue.StorageClass()->modifyStorageClassName(admin, currentName, newName);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
