/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/RequesterActivityMountRuleCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class RequesterActivityMountRuleCatalogueRetryWrapper : public RequesterActivityMountRuleCatalogue {
public:
  RequesterActivityMountRuleCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~RequesterActivityMountRuleCatalogueRetryWrapper() override = default;

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

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
