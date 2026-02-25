/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/RequesterMountRuleCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

RequesterMountRuleCatalogueRetryWrapper::RequesterMountRuleCatalogueRetryWrapper(Catalogue& catalogue,
                                                                                 log::Logger& log,
                                                                                 const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void RequesterMountRuleCatalogueRetryWrapper::modifyRequesterMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& mountPolicy) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterName, &mountPolicy]() {
      m_catalogue.RequesterMountRule()->modifyRequesterMountRulePolicy(admin, instanceName, requesterName, mountPolicy);
    },
    m_maxTriesToConnect);
}

void RequesterMountRuleCatalogueRetryWrapper::modifyRequesterMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterName, &comment]() {
      m_catalogue.RequesterMountRule()->modifyRequesterMountRuleComment(admin, instanceName, requesterName, comment);
    },
    m_maxTriesToConnect);
}

void RequesterMountRuleCatalogueRetryWrapper::createRequesterMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstance,
  const std::string& requesterName,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &mountPolicyName, &diskInstance, &requesterName, &comment]() {
      m_catalogue.RequesterMountRule()->createRequesterMountRule(admin,
                                                                 mountPolicyName,
                                                                 diskInstance,
                                                                 requesterName,
                                                                 comment);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::RequesterMountRule>
RequesterMountRuleCatalogueRetryWrapper::getRequesterMountRules() const {
  return retryOnLostConnection(
    m_log,
    [this]() { return m_catalogue.RequesterMountRule()->getRequesterMountRules(); },
    m_maxTriesToConnect);
}

void RequesterMountRuleCatalogueRetryWrapper::deleteRequesterMountRule(const std::string& diskInstanceName,
                                                                       const std::string& requesterName) {
  return retryOnLostConnection(
    m_log,
    [this, &diskInstanceName, &requesterName]() {
      m_catalogue.RequesterMountRule()->deleteRequesterMountRule(diskInstanceName, requesterName);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
