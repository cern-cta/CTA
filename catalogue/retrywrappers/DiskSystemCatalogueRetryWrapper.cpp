/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/DiskSystemCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/log/Logger.hpp"
#include "disk/DiskSystem.hpp"

#include <memory>

namespace cta::catalogue {

DiskSystemCatalogueRetryWrapper::DiskSystemCatalogueRetryWrapper(Catalogue& catalogue,
                                                                 log::Logger& log,
                                                                 const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void DiskSystemCatalogueRetryWrapper::createDiskSystem(const common::dataStructures::SecurityIdentity& admin,
                                                       const std::string& name,
                                                       const std::string& diskInstanceName,
                                                       const std::string& diskInstanceSpaceName,
                                                       const std::string& fileRegexp,
                                                       const uint64_t targetedFreeSpace,
                                                       const time_t sleepTime,
                                                       const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this,
     &admin,
     &name,
     &diskInstanceName,
     &diskInstanceSpaceName,
     &fileRegexp,
     &targetedFreeSpace,
     &sleepTime,
     &comment] {
      return m_catalogue.DiskSystem()->createDiskSystem(admin,
                                                        name,
                                                        diskInstanceName,
                                                        diskInstanceSpaceName,
                                                        fileRegexp,
                                                        targetedFreeSpace,
                                                        sleepTime,
                                                        comment);
    },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::deleteDiskSystem(const std::string& name) {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.DiskSystem()->deleteDiskSystem(name); },
    m_maxTriesToConnect);
}

disk::DiskSystemList DiskSystemCatalogueRetryWrapper::getAllDiskSystems() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.DiskSystem()->getAllDiskSystems(); },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity& admin,
                                                                 const std::string& name,
                                                                 const std::string& fileRegexp) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &fileRegexp] {
      return m_catalogue.DiskSystem()->modifyDiskSystemFileRegexp(admin, name, fileRegexp);
    },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemTargetedFreeSpace(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t targetedFreeSpace) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &targetedFreeSpace] {
      return m_catalogue.DiskSystem()->modifyDiskSystemTargetedFreeSpace(admin, name, targetedFreeSpace);
    },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity& admin,
                                                              const std::string& name,
                                                              const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &comment] { return m_catalogue.DiskSystem()->modifyDiskSystemComment(admin, name, comment); },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& name,
                                                                const uint64_t sleepTime) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &sleepTime] {
      return m_catalogue.DiskSystem()->modifyDiskSystemSleepTime(admin, name, sleepTime);
    },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemDiskInstanceName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstanceName) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &diskInstanceName] {
      return m_catalogue.DiskSystem()->modifyDiskSystemDiskInstanceName(admin, name, diskInstanceName);
    },
    m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemDiskInstanceSpaceName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& diskInstanceSpaceName) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &diskInstanceSpaceName] {
      return m_catalogue.DiskSystem()->modifyDiskSystemDiskInstanceSpaceName(admin, name, diskInstanceSpaceName);
    },
    m_maxTriesToConnect);
}

bool DiskSystemCatalogueRetryWrapper::diskSystemExists(const std::string& name) const {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.DiskSystem()->diskSystemExists(name); },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
