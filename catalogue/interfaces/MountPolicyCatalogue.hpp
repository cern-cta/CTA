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

#include <list>
#include <optional>
#include <string>

namespace cta {

namespace common {
namespace dataStructures {
struct MountPolicy;
struct SecurityIdentity;
}  // namespace dataStructures
}  // namespace common

namespace catalogue {

class CreateMountPolicyAttributes;

class MountPolicyCatalogue {
public:
  virtual ~MountPolicyCatalogue() = default;

  virtual void createMountPolicy(const common::dataStructures::SecurityIdentity& admin,
                                 const CreateMountPolicyAttributes& mountPolicy) = 0;

  /**
   * Returns the list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  virtual std::list<common::dataStructures::MountPolicy> getMountPolicies() const = 0;

  /**
   * Returns the mount policy with the specified name.
   *
   * @return the specified mount policy
   */
  virtual std::optional<common::dataStructures::MountPolicy>
    getMountPolicy(const std::string& mountPolicyName) const = 0;

  /**
   * Returns the cached list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  virtual std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const = 0;

  /**
   * Deletes the specified mount policy.
   *
   * @param name The name of the mount policy.
   */
  virtual void deleteMountPolicy(const std::string& name) = 0;

  virtual void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& name,
                                                const uint64_t archivePriority) = 0;

  virtual void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const uint64_t minArchiveRequestAge) = 0;

  virtual void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin,
                                                 const std::string& name,
                                                 const uint64_t retrievePriority) = 0;

  virtual void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& name,
                                                      const uint64_t minRetrieveRequestAge) = 0;

  virtual void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin,
                                        const std::string& name,
                                        const std::string& comment) = 0;
};

}  // namespace catalogue
}  // namespace cta