/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/RequesterActivityMountRuleCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

RequesterActivityMountRuleCatalogueRetryWrapper::RequesterActivityMountRuleCatalogueRetryWrapper(
  Catalogue& catalogue,
  log::Logger& log,
  const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void RequesterActivityMountRuleCatalogueRetryWrapper::modifyRequesterActivityMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& mountPolicy) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterName, &activityRegex, &mountPolicy]() {
      m_catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(admin,
                                                                                       instanceName,
                                                                                       requesterName,
                                                                                       activityRegex,
                                                                                       mountPolicy);
    },
    m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::modifyRequesterActivityMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &instanceName, &requesterName, &activityRegex, &comment]() {
      m_catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(admin,
                                                                                        instanceName,
                                                                                        requesterName,
                                                                                        activityRegex,
                                                                                        comment);
    },
    m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstance,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &mountPolicyName, &diskInstance, &requesterName, &activityRegex, &comment]() {
      m_catalogue.RequesterActivityMountRule()
        ->createRequesterActivityMountRule(admin, mountPolicyName, diskInstance, requesterName, activityRegex, comment);
    },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::RequesterActivityMountRule>
RequesterActivityMountRuleCatalogueRetryWrapper::getRequesterActivityMountRules() const {
  return retryOnLostConnection(
    m_log,
    [this]() { return m_catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules(); },
    m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::deleteRequesterActivityMountRule(
  const std::string& diskInstanceName,
  const std::string& requesterName,
  const std::string& activityRegex) {
  return retryOnLostConnection(
    m_log,
    [this, &diskInstanceName, &requesterName, &activityRegex]() {
      m_catalogue.RequesterActivityMountRule()->deleteRequesterActivityMountRule(diskInstanceName,
                                                                                 requesterName,
                                                                                 activityRegex);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
