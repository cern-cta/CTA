/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyRequesterActivityMountRuleCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <string>

namespace cta::catalogue {

void DummyRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& mountPolicy) {
  throw exception::NotImplementedException();
}

void DummyRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyRequesterActivityMountRuleCatalogue::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstance,
  const std::string& requesterName,
  const std::string& activityRegex,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::RequesterActivityMountRule>
DummyRequesterActivityMountRuleCatalogue::getRequesterActivityMountRules() const {
  throw exception::NotImplementedException();
}

void DummyRequesterActivityMountRuleCatalogue::deleteRequesterActivityMountRule(const std::string& diskInstanceName,
                                                                                const std::string& requesterName,
                                                                                const std::string& activityRegex) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
