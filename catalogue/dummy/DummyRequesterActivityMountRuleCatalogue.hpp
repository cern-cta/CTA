/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/interfaces/RequesterActivityMountRuleCatalogue.hpp"

namespace cta::catalogue {

class DummyRequesterActivityMountRuleCatalogue : public RequesterActivityMountRuleCatalogue {
public:
  DummyRequesterActivityMountRuleCatalogue() = default;
  ~DummyRequesterActivityMountRuleCatalogue() override = default;

  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex,
    const std::string &mountPolicy) override;

  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex,
    const std::string &comment) override;

  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName,
    const std::string &activityRegex, const std::string &comment) override;

  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const override;

  void deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName,
    const std::string &activityRegex) override;
};

} // namespace cta::catalogue
