/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <optional>
#include <string>

#include "catalogue/dummy/DummyRequesterMountRuleCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyRequesterMountRuleCatalogue::modifyRequesterMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &mountPolicy) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterMountRuleCatalogue::modifyRequesteMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterMountRuleCatalogue::createRequesterMountRule(const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName,
  const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::RequesterMountRule> DummyRequesterMountRuleCatalogue::getRequesterMountRules() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyRequesterMountRuleCatalogue::deleteRequesterMountRule(const std::string &diskInstanceName,
  const std::string &requesterName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
