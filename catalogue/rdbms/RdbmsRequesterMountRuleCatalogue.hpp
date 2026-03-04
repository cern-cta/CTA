/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace rdbms {
class ConnPool;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsRequesterMountRuleCatalogue : public RequesterMountRuleCatalogue {
public:
  RdbmsRequesterMountRuleCatalogue(log::Logger& log,
                                   std::shared_ptr<rdbms::ConnPool> connPool,
                                   RdbmsCatalogue* rdbmsCatalogue);

  ~RdbmsRequesterMountRuleCatalogue() override = default;

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

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;
};

}  // namespace catalogue
}  // namespace cta
