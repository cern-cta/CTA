/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <string>

namespace cta {

namespace common::dataStructures {
class SecurityIdentity;
struct RequesterGroupMountRule;
}

namespace catalogue {

class RequesterGroupMountRuleCatalogue {
public:
  virtual ~RequesterGroupMountRuleCatalogue() = default;

  virtual void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) = 0;

  virtual void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) = 0;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester group.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guarantted to be unique within its disk instance.
   * @param comment Comment.
   */
  virtual void createRequesterGroupMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstanceName,
    const std::string &requesterGroupName,
    const std::string &comment) = 0;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester group.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester group.
   */
  virtual std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const = 0;

  /**
   * Deletes the specified mount rule.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   */
  virtual void deleteRequesterGroupMountRule(
    const std::string &diskInstanceName,
    const std::string &requesterGroupName) = 0;
};

}} // namespace cta::catalogue
