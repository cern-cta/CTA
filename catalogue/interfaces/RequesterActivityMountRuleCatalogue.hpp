/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta {

namespace common::dataStructures {
struct RequesterActivityMountRule;
struct SecurityIdentity;
}

namespace catalogue {

class RequesterActivityMountRuleCatalogue {
public:
  virtual ~RequesterActivityMountRuleCatalogue() = default;

  virtual void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex,
    const std::string &mountPolicy) = 0;

  virtual void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex,
    const std::string &comment) = 0;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester+matching activities.
   *
   * Please note that requester-activity mount-rules overrule requester
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstance The name of the disk instance to which the requester
   * belongs.
   * @param activityRegex The regex to match request activities
   * @param requesterName The name of the requester which is only guarantted to
   * be unique within its disk instance.
   * @param comment Comment.
   */
  virtual void createRequesterActivityMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstance,
    const std::string &requesterName,
    const std::string &activityRegex,
    const std::string &comment) = 0;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester + activity.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester + activity.
   */
  virtual std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const = 0;

    /**
   * Deletes the specified mount rule.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   * @param activityRegex The regex to match request activities
   */
  virtual void deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName,
    const std::string &activityRegex) = 0;
};

}} // namespace cta::catalogue
