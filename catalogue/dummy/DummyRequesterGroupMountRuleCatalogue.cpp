/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <optional>
#include <string>

#include "catalogue/dummy/DummyRequesterGroupMountRuleCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterGroupName, const std::string &mountPolicy) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterGroupName, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterGroupMountRuleCatalogue::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName,
  const std::string &diskInstanceName, const std::string &requesterGroupName, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::RequesterGroupMountRule>
  DummyRequesterGroupMountRuleCatalogue::getRequesterGroupMountRules() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}


void DummyRequesterGroupMountRuleCatalogue::deleteRequesterGroupMountRule(const std::string &diskInstanceName,
  const std::string &requesterGroupName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
