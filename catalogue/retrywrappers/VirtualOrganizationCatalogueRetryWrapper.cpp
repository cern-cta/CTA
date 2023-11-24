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
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/VirtualOrganizationCatalogueRetryWrapper.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

namespace cta::catalogue {

VirtualOrganizationCatalogueRetryWrapper::VirtualOrganizationCatalogueRetryWrapper(
  const std::unique_ptr<Catalogue>& catalogue, log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void VirtualOrganizationCatalogueRetryWrapper::createVirtualOrganization(
  const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) {
  return retryOnLostConnection(m_log, [this,&admin,&vo] {
    return m_catalogue->VO()->createVirtualOrganization(admin, vo);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::deleteVirtualOrganization(const std::string &voName) {
  return retryOnLostConnection(m_log, [this,&voName] {
    return m_catalogue->VO()->deleteVirtualOrganization(voName);
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::VirtualOrganization> VirtualOrganizationCatalogueRetryWrapper::getVirtualOrganizations() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->VO()->getVirtualOrganizations();
  }, m_maxTriesToConnect);
}

common::dataStructures::VirtualOrganization VirtualOrganizationCatalogueRetryWrapper::getVirtualOrganizationOfTapepool(
  const std::string & tapepoolName) const {
  return retryOnLostConnection(m_log, [this,&tapepoolName] {
    return m_catalogue->VO()->getVirtualOrganizationOfTapepool(tapepoolName);
  }, m_maxTriesToConnect);
}

common::dataStructures::VirtualOrganization VirtualOrganizationCatalogueRetryWrapper::getCachedVirtualOrganizationOfTapepool(
  const std::string & tapepoolName) const {
  return retryOnLostConnection(m_log, [this,&tapepoolName] {
    return m_catalogue->VO()->getCachedVirtualOrganizationOfTapepool(tapepoolName);
  }, m_maxTriesToConnect);
}

std::optional<common::dataStructures::VirtualOrganization> VirtualOrganizationCatalogueRetryWrapper::getDefaultVirtualOrganizationForRepack() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) {
  return retryOnLostConnection(m_log, [this,&admin,&currentVoName,&newVoName] {
    return m_catalogue->VO()->modifyVirtualOrganizationName(admin, currentVoName, newVoName);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationReadMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&readMaxDrives] {
    return m_catalogue->VO()->modifyVirtualOrganizationReadMaxDrives(admin, voName, readMaxDrives);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationWriteMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&writeMaxDrives] {
    return m_catalogue->VO()->modifyVirtualOrganizationWriteMaxDrives(admin, voName, writeMaxDrives);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationMaxFileSize(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&maxFileSize] {
    return m_catalogue->VO()->modifyVirtualOrganizationMaxFileSize(admin, voName, maxFileSize);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&comment] {
    return m_catalogue->VO()->modifyVirtualOrganizationComment(admin, voName, comment);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationDiskInstanceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&diskInstance] {
    return m_catalogue->VO()->modifyVirtualOrganizationDiskInstanceName(admin, voName, diskInstance);
  }, m_maxTriesToConnect);
}

void VirtualOrganizationCatalogueRetryWrapper::modifyVirtualOrganizationIsRepackVo(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const bool isRepackVo) {
  return retryOnLostConnection(m_log, [this,&admin,&voName,&isRepackVo] {
    return m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(admin, voName, isRepackVo);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
