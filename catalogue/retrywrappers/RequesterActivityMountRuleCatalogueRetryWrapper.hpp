/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterActivityMountRuleCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class RequesterActivityMountRuleCatalogueRetryWrapper : public RequesterActivityMountRuleCatalogue {
public:
  RequesterActivityMountRuleCatalogueRetryWrapper(Catalogue& catalogue,
                                                  log::Logger& m_log,
                                                  const uint32_t maxTriesToConnect);
  ~RequesterActivityMountRuleCatalogueRetryWrapper() override = default;

  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& instanceName,
                                              const std::string& requesterName,
                                              const std::string& activityRegex,
                                              const std::string& mountPolicy) override;

  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity& admin,
                                               const std::string& instanceName,
                                               const std::string& requesterName,
                                               const std::string& activityRegex,
                                               const std::string& comment) override;

  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity& admin,
                                        const std::string& mountPolicyName,
                                        const std::string& diskInstance,
                                        const std::string& requesterName,
                                        const std::string& activityRegex,
                                        const std::string& comment) override;

  std::vector<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const override;

  void deleteRequesterActivityMountRule(const std::string& diskInstanceName,
                                        const std::string& requesterName,
                                        const std::string& activityRegex) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class RequesterActivityMountRuleCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
