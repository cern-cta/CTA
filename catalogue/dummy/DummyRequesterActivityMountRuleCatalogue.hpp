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

#include "catalogue/interfaces/RequesterActivityMountRuleCatalogue.hpp"

namespace cta {
namespace catalogue {

class DummyRequesterActivityMountRuleCatalogue : public RequesterActivityMountRuleCatalogue {
public:
  DummyRequesterActivityMountRuleCatalogue() = default;
  ~DummyRequesterActivityMountRuleCatalogue() override = default;

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

  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const override;

  void deleteRequesterActivityMountRule(const std::string& diskInstanceName,
                                        const std::string& requesterName,
                                        const std::string& activityRegex) override;
};

}  // namespace catalogue
}  // namespace cta
