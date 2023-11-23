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
#include "catalogue/retrywrappers/RequesterGroupMountRuleCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"

namespace cta::catalogue {

RequesterGroupMountRuleCatalogueRetryWrapper::RequesterGroupMountRuleCatalogueRetryWrapper(
  const std::unique_ptr<Catalogue>& catalogue, log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void RequesterGroupMountRuleCatalogueRetryWrapper::modifyRequesterGroupMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterGroupName, const std::string &mountPolicy) {
  return retryOnLostConnection(m_log, [&] {
    return m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(admin, instanceName,
    requesterGroupName, mountPolicy);}, m_maxTriesToConnect);
}

void RequesterGroupMountRuleCatalogueRetryWrapper::modifyRequesterGroupMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterGroupName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&] {
    return m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(admin, instanceName,
    requesterGroupName, comment);}, m_maxTriesToConnect);
}

void RequesterGroupMountRuleCatalogueRetryWrapper::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName,
  const std::string &diskInstanceName, const std::string &requesterGroupName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&] {
    return m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(admin, mountPolicyName,
    diskInstanceName, requesterGroupName, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::RequesterGroupMountRule>
  RequesterGroupMountRuleCatalogueRetryWrapper::getRequesterGroupMountRules() const {
  return retryOnLostConnection(m_log, [&] {
    return m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();}, m_maxTriesToConnect);
}


void RequesterGroupMountRuleCatalogueRetryWrapper::deleteRequesterGroupMountRule(const std::string &diskInstanceName,
  const std::string &requesterGroupName) {
  return retryOnLostConnection(m_log, [&] {
    return m_catalogue->RequesterGroupMountRule()->deleteRequesterGroupMountRule(diskInstanceName,
    requesterGroupName);}, m_maxTriesToConnect);
}

} // namespace cta::catalogue
