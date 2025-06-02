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

namespace cta {

namespace common::dataStructures {
struct RequesterActivityMountRule;
struct SecurityIdentity;
}  // namespace common::dataStructures

namespace catalogue {

class RequesterActivityMountRuleCatalogue {
public:
  virtual ~RequesterActivityMountRuleCatalogue() = default;

  virtual void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& instanceName,
                                                      const std::string& requesterName,
                                                      const std::string& activityRegex,
                                                      const std::string& mountPolicy) = 0;

  virtual void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity& admin,
                                                       const std::string& instanceName,
                                                       const std::string& requesterName,
                                                       const std::string& activityRegex,
                                                       const std::string& comment) = 0;

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
  virtual void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& mountPolicyName,
                                                const std::string& diskInstance,
                                                const std::string& requesterName,
                                                const std::string& activityRegex,
                                                const std::string& comment) = 0;

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
  virtual void deleteRequesterActivityMountRule(const std::string& diskInstanceName,
                                                const std::string& requesterName,
                                                const std::string& activityRegex) = 0;
};

}  // namespace catalogue
}  // namespace cta
