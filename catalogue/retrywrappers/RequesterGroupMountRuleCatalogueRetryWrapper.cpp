/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/RequesterGroupMountRuleCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

RequesterGroupMountRuleCatalogueRetryWrapper::RequesterGroupMountRuleCatalogueRetryWrapper(
  Catalogue& catalogue,
  log::Logger& log,
  const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void RequesterGroupMountRuleCatalogueRetryWrapper::modifyRequesterGroupMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& mountPolicy) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterGroupName, &mountPolicy] {
      return m_catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(admin,
                                                                                        instanceName,
                                                                                        requesterGroupName,
                                                                                        mountPolicy);
    },
    m_maxTriesToConnect);
}

void RequesterGroupMountRuleCatalogueRetryWrapper::modifyRequesterGroupMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterGroupName, &comment] {
      return m_catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(admin,
                                                                                         instanceName,
                                                                                         requesterGroupName,
                                                                                         comment);
    },
    m_maxTriesToConnect);
}

void RequesterGroupMountRuleCatalogueRetryWrapper::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &mountPolicyName, &diskInstanceName, &requesterGroupName, &comment] {
      return m_catalogue.RequesterGroupMountRule()->createRequesterGroupMountRule(admin,
                                                                                  mountPolicyName,
                                                                                  diskInstanceName,
                                                                                  requesterGroupName,
                                                                                  comment);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::RequesterGroupMountRule>
RequesterGroupMountRuleCatalogueRetryWrapper::getRequesterGroupMountRules() const {
  return retryOnLostConnection(
    m_log,
    [this] { return m_catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules(); },
    m_maxTriesToConnect);
}

void RequesterGroupMountRuleCatalogueRetryWrapper::deleteRequesterGroupMountRule(
  const std::string& diskInstanceName,
  const std::string& requesterGroupName) {
  return retryOnLostConnection(
    m_log,
    [this, &diskInstanceName, &requesterGroupName] {
      return m_catalogue.RequesterGroupMountRule()->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
