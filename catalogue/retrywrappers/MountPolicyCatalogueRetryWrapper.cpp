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
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/MountPolicyCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"

namespace cta {
namespace catalogue {

MountPolicyCatalogueRetryWrapper::MountPolicyCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void MountPolicyCatalogueRetryWrapper::createMountPolicy(const common::dataStructures::SecurityIdentity &admin,
  const CreateMountPolicyAttributes & mountPolicy) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->createMountPolicy(admin, mountPolicy);},
    m_maxTriesToConnect);
}

std::list<common::dataStructures::MountPolicy> MountPolicyCatalogueRetryWrapper::getMountPolicies() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->getMountPolicies();},
    m_maxTriesToConnect);
}

std::optional<common::dataStructures::MountPolicy> MountPolicyCatalogueRetryWrapper::getMountPolicy(
  const std::string &mountPolicyName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->getMountPolicy(mountPolicyName);},
    m_maxTriesToConnect);
}


std::list<common::dataStructures::MountPolicy> MountPolicyCatalogueRetryWrapper::getCachedMountPolicies() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->getCachedMountPolicies();},
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::deleteMountPolicy(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->deleteMountPolicy(name);},
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyArchivePriority(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->modifyMountPolicyArchivePriority(admin,
    name, archivePriority);}, m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyArchiveMinRequestAge(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->modifyMountPolicyArchiveMinRequestAge(
    admin, name, minArchiveRequestAge);}, m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyRetrievePriority(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->modifyMountPolicyRetrievePriority(admin,
    name, retrievePriority);}, m_maxTriesToConnect);
}
void MountPolicyCatalogueRetryWrapper::modifyMountPolicyRetrieveMinRequestAge(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const uint64_t minRetrieveRequestAge) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(
    admin, name, minRetrieveRequestAge);}, m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->MountPolicy()->modifyMountPolicyComment(admin, name,
    comment);}, m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta