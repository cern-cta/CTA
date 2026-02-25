/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/MountPolicyCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

MountPolicyCatalogueRetryWrapper::MountPolicyCatalogueRetryWrapper(Catalogue& catalogue,
                                                                   log::Logger& log,
                                                                   const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void MountPolicyCatalogueRetryWrapper::createMountPolicy(const common::dataStructures::SecurityIdentity& admin,
                                                         const CreateMountPolicyAttributes& mountPolicy) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &mountPolicy] { return m_catalogue.MountPolicy()->createMountPolicy(admin, mountPolicy); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::MountPolicy> MountPolicyCatalogueRetryWrapper::getMountPolicies() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.MountPolicy()->getMountPolicies(); },
    m_maxTriesToConnect);
}

std::optional<common::dataStructures::MountPolicy>
MountPolicyCatalogueRetryWrapper::getMountPolicy(const std::string& mountPolicyName) const {
  return retryOnLostConnection(
    m_log,
    [this, &mountPolicyName] { return m_catalogue.MountPolicy()->getMountPolicy(mountPolicyName); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::MountPolicy> MountPolicyCatalogueRetryWrapper::getCachedMountPolicies() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.MountPolicy()->getCachedMountPolicies(); },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::deleteMountPolicy(const std::string& name) {
  return retryOnLostConnection(
    m_log,
    [this, &name] { return m_catalogue.MountPolicy()->deleteMountPolicy(name); },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyArchivePriority(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t archivePriority) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &archivePriority] {
      return m_catalogue.MountPolicy()->modifyMountPolicyArchivePriority(admin, name, archivePriority);
    },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyArchiveMinRequestAge(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t minArchiveRequestAge) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &minArchiveRequestAge] {
      return m_catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(admin, name, minArchiveRequestAge);
    },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyRetrievePriority(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t retrievePriority) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &retrievePriority] {
      return m_catalogue.MountPolicy()->modifyMountPolicyRetrievePriority(admin, name, retrievePriority);
    },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyRetrieveMinRequestAge(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t minRetrieveRequestAge) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &minRetrieveRequestAge] {
      return m_catalogue.MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(admin, name, minRetrieveRequestAge);
    },
    m_maxTriesToConnect);
}

void MountPolicyCatalogueRetryWrapper::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& name,
                                                                const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &name, &comment] {
      return m_catalogue.MountPolicy()->modifyMountPolicyComment(admin, name, comment);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
