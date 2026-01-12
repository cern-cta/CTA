/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyRequesterGroupMountRuleCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <optional>
#include <string>

namespace cta::catalogue {

void DummyRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& mountPolicy) {
  throw exception::NotImplementedException();
}

void DummyRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyRequesterGroupMountRuleCatalogue::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::RequesterGroupMountRule>
DummyRequesterGroupMountRuleCatalogue::getRequesterGroupMountRules() const {
  throw exception::NotImplementedException();
}

void DummyRequesterGroupMountRuleCatalogue::deleteRequesterGroupMountRule(const std::string& diskInstanceName,
                                                                          const std::string& requesterGroupName) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
