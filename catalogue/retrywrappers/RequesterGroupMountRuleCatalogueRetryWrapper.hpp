/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class RequesterGroupMountRuleCatalogueRetryWrapper : public RequesterGroupMountRuleCatalogue {
public:
  RequesterGroupMountRuleCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                               log::Logger& m_log,
                                               const uint32_t maxTriesToConnect);
  ~RequesterGroupMountRuleCatalogueRetryWrapper() override = default;

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

  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override;

  void deleteRequesterGroupMountRule(const std::string& diskInstanceName,
                                     const std::string& requesterGroupName) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
