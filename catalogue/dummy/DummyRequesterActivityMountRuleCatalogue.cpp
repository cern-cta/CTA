/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <optional>
#include <string>

#include "catalogue/dummy/DummyRequesterActivityMountRuleCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterActivityMountRuleCatalogue::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName,
  const std::string &diskInstance, const std::string &requesterName, const std::string &activityRegex,
  const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::RequesterActivityMountRule>
  DummyRequesterActivityMountRuleCatalogue::getRequesterActivityMountRules() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterActivityMountRuleCatalogue::deleteRequesterActivityMountRule(const std::string &diskInstanceName,
  const std::string &requesterName, const std::string &activityRegex) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
