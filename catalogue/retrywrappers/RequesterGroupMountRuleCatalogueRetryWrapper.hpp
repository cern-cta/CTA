/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"

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