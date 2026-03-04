/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class RequesterMountRuleCatalogueRetryWrapper : public RequesterMountRuleCatalogue {
public:
  RequesterMountRuleCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~RequesterMountRuleCatalogueRetryWrapper() override = default;

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
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class RequesterMountRuleCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
