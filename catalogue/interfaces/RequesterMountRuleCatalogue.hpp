/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <vector>

namespace cta {

namespace common::dataStructures {
class SecurityIdentity;
struct RequesterMountRule;
}  // namespace common::dataStructures

namespace catalogue {

class RequesterMountRuleCatalogue {
public:
  virtual ~RequesterMountRuleCatalogue() = default;

  virtual void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& instanceName,
                                              const std::string& requesterName,
                                              const std::string& mountPolicy) = 0;

  virtual void modifyRequesterMountRuleComment(const common::dataStructures::SecurityIdentity& admin,
                                               const std::string& instanceName,
                                               const std::string& requesterName,
                                               const std::string& comment) = 0;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstance The name of the disk instance to which the requester
   * belongs.
   * @param requesterName The name of the requester which is only guarantted to
   * be unique within its disk instance.
   * @param comment Comment.
   */
  virtual void createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin,
                                        const std::string& mountPolicyName,
                                        const std::string& diskInstance,
                                        const std::string& requesterName,
                                        const std::string& comment) = 0;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester.
   */
  virtual std::vector<common::dataStructures::RequesterMountRule> getRequesterMountRules() const = 0;

  /**
   * Deletes the specified mount rule.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   */
  virtual void deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) = 0;
};

}  // namespace catalogue
}  // namespace cta
