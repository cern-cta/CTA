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
#include "catalogue/retrywrappers/DiskSystemCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/log/Logger.hpp"
#include "disk/DiskSystem.hpp"

namespace cta {
namespace catalogue {

DiskSystemCatalogueRetryWrapper::DiskSystemCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void DiskSystemCatalogueRetryWrapper::createDiskSystem(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName,
  const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime,const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->createDiskSystem(admin, name,
    diskInstanceName, diskInstanceSpaceName, fileRegexp, targetedFreeSpace, sleepTime, comment);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::deleteDiskSystem(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->deleteDiskSystem(name);},
    m_maxTriesToConnect);
}

disk::DiskSystemList DiskSystemCatalogueRetryWrapper::getAllDiskSystems() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->getAllDiskSystems();}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &fileRegexp) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemFileRegexp(admin, name,
    fileRegexp);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemTargetedFreeSpace(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const uint64_t targetedFreeSpace) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemTargetedFreeSpace(admin,
    name, targetedFreeSpace);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemComment(admin, name,
    comment);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
  const std::string& name, const uint64_t sleepTime) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemSleepTime(admin, name,
    sleepTime);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemDiskInstanceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemDiskInstanceName(admin,
    name, diskInstanceName);}, m_maxTriesToConnect);
}

void DiskSystemCatalogueRetryWrapper::modifyDiskSystemDiskInstanceSpaceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const std::string &diskInstanceSpaceName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->modifyDiskSystemDiskInstanceSpaceName(admin,
    name, diskInstanceSpaceName);}, m_maxTriesToConnect);
}

bool DiskSystemCatalogueRetryWrapper::diskSystemExists(const std::string &name) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DiskSystem()->diskSystemExists(name);},
    m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta