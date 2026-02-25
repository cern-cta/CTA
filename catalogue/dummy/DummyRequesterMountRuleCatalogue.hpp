/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"

namespace cta::catalogue {

class DummyRequesterMountRuleCatalogue : public RequesterMountRuleCatalogue {
public:
  DummyRequesterMountRuleCatalogue() = default;
  ~DummyRequesterMountRuleCatalogue() override = default;

  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin,
                                      const std::string& instanceName,
                                      const std::string& requesterName,
                                      const std::string& mountPolicy) override;

  void modifyRequesterMountRuleComment(const common::dataStructures::SecurityIdentity& admin,
                                       const std::string& instanceName,
                                       const std::string& requesterName,
                                       const std::string& comment) override;

  void createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin,
                                const std::string& mountPolicyName,
                                const std::string& diskInstance,
                                const std::string& requesterName,
                                const std::string& comment) override;

  std::vector<common::dataStructures::RequesterMountRule> getRequesterMountRules() const override;

  void deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) override;
};

}  // namespace cta::catalogue
