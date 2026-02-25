/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/DiskInstanceSpaceCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta::catalogue {

DiskInstanceSpaceCatalogueRetryWrapper::DiskInstanceSpaceCatalogueRetryWrapper(Catalogue& catalogue,
                                                                               log::Logger& log,
                                                                               const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void DiskInstanceSpaceCatalogueRetryWrapper::deleteDiskInstanceSpace(const std::string& name,
                                                                     const std::string& diskInstance) {
  return retryOnLostConnection(
    m_log,
    [this, &name, &diskInstance] {
      return m_catalogue.DiskInstanceSpace()->deleteDiskInstanceSpace(name, diskInstance);
    },
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::createDiskInstanceSpace(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstance,
  const std::string& freeSpaceQueryURL,
  const uint64_t refreshInterval,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, name, &diskInstance, &freeSpaceQueryURL, &refreshInterval, &comment] {
      return m_catalogue.DiskInstanceSpace()
        ->createDiskInstanceSpace(admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::DiskInstanceSpace>
DiskInstanceSpaceCatalogueRetryWrapper::getAllDiskInstanceSpaces() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces(); },
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstance,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &diskInstance, &comment] {
      return m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceComment(admin, name, diskInstance, comment);
    },
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceRefreshInterval(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstance,
  const uint64_t refreshInterval) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &diskInstance, &refreshInterval] {
      return m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(admin,
                                                                                     name,
                                                                                     diskInstance,
                                                                                     refreshInterval);
    },
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceFreeSpace(const std::string& name,
                                                                              const std::string& diskInstance,
                                                                              const uint64_t freeSpace) {
  return retryOnLostConnection(
    m_log,
    [this, &name, &diskInstance, &freeSpace] {
      return m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceFreeSpace(name, diskInstance, freeSpace);
    },
    m_maxTriesToConnect);
}

void DiskInstanceSpaceCatalogueRetryWrapper::modifyDiskInstanceSpaceQueryURL(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstance,
  const std::string& freeSpaceQueryURL) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &diskInstance, &freeSpaceQueryURL] {
      return m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(admin,
                                                                              name,
                                                                              diskInstance,
                                                                              freeSpaceQueryURL);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
