/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace rdbms {
class ConnPool;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsRequesterGroupMountRuleCatalogue : public RequesterGroupMountRuleCatalogue {
public:
  RdbmsRequesterGroupMountRuleCatalogue(log::Logger& log,
                                        std::shared_ptr<rdbms::ConnPool> connPool,
                                        RdbmsCatalogue* rdbmsCatalogue);

  ~RdbmsRequesterGroupMountRuleCatalogue() override = default;

  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& instanceName,
                                           const std::string& requesterGroupName,
                                           const std::string& mountPolicy) override;

  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& instanceName,
                                            const std::string& requesterGroupName,
                                            const std::string& comment) override;

  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity& admin,
                                     const std::string& mountPolicyName,
                                     const std::string& diskInstanceName,
                                     const std::string& requesterGroupName,
                                     const std::string& comment) override;

  std::vector<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override;

  void deleteRequesterGroupMountRule(const std::string& diskInstanceName,
                                     const std::string& requesterGroupName) override;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;
};

}  // namespace catalogue
}  // namespace cta
