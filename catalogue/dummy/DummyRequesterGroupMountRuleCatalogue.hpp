/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"

namespace cta::catalogue {

class DummyRequesterGroupMountRuleCatalogue : public RequesterGroupMountRuleCatalogue {
public:
  DummyRequesterGroupMountRuleCatalogue() = default;
  ~DummyRequesterGroupMountRuleCatalogue() override = default;

  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) override;

  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) override;

  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName, const std::string &diskInstanceName, const std::string &requesterGroupName,
    const std::string &comment) override;

  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override;


  void deleteRequesterGroupMountRule(const std::string &diskInstanceName,
    const std::string &requesterGroupName) override;
};

} // namespace cta::catalogue
