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
#include "catalogue/retrywrappers/RequesterActivityMountRuleCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"

namespace cta::catalogue {

RequesterActivityMountRuleCatalogueRetryWrapper::RequesterActivityMountRuleCatalogueRetryWrapper(
  const std::unique_ptr<Catalogue>& catalogue, log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void RequesterActivityMountRuleCatalogueRetryWrapper::modifyRequesterActivityMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) {
  return retryOnLostConnection(m_log, [this,&admin,&instanceName,&requesterName,&activityRegex,&mountPolicy]() {
    m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(admin, instanceName, requesterName, activityRegex, mountPolicy);
  }, m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::modifyRequesterActivityMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&instanceName,&requesterName,&activityRegex,&comment]() {
    m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(admin, instanceName, requesterName, activityRegex, comment);
  }, m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName,
  const std::string &diskInstance, const std::string &requesterName, const std::string &activityRegex,
  const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&mountPolicyName,&diskInstance,&requesterName,&activityRegex,&comment]() {
    m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(admin, mountPolicyName, diskInstance, requesterName, activityRegex, comment);
  }, m_maxTriesToConnect);
}

std::list<common::dataStructures::RequesterActivityMountRule>
  RequesterActivityMountRuleCatalogueRetryWrapper::getRequesterActivityMountRules() const {
  return retryOnLostConnection(m_log, [this]() {
    return m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
  }, m_maxTriesToConnect);
}

void RequesterActivityMountRuleCatalogueRetryWrapper::deleteRequesterActivityMountRule(
  const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) {
  return retryOnLostConnection(m_log, [this,&diskInstanceName,&requesterName,&activityRegex]() {
    m_catalogue->RequesterActivityMountRule()->deleteRequesterActivityMountRule(diskInstanceName, requesterName, activityRegex);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue
