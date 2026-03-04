/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyRequesterMountRuleCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <optional>
#include <string>

namespace cta::catalogue {

void DummyRequesterMountRuleCatalogue::modifyRequesterMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& mountPolicy) {
  throw exception::NotImplementedException();
}

void DummyRequesterMountRuleCatalogue::modifyRequesterMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterName,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyRequesterMountRuleCatalogue::createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& mountPolicyName,
                                                                const std::string& diskInstance,
                                                                const std::string& requesterName,
                                                                const std::string& comment) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::RequesterMountRule>
DummyRequesterMountRuleCatalogue::getRequesterMountRules() const {
  throw exception::NotImplementedException();
}

void DummyRequesterMountRuleCatalogue::deleteRequesterMountRule(const std::string& diskInstanceName,
                                                                const std::string& requesterName) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
