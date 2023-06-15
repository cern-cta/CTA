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
#include <memory>
#include <optional>
#include <string>

#include "catalogue/interfaces/MountPolicyCatalogue.hpp"
#include "catalogue/RequesterAndGroupMountPolicies.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

class RdbmsCatalogue;

class RdbmsMountPolicyCatalogue : public MountPolicyCatalogue {
public:
  RdbmsMountPolicyCatalogue(log::Logger& log,
                            std::shared_ptr<rdbms::ConnPool> connPool,
                            RdbmsCatalogue* rdbmsCatalogue);
  ~RdbmsMountPolicyCatalogue() override = default;

  void createMountPolicy(const common::dataStructures::SecurityIdentity& admin,
                         const CreateMountPolicyAttributes& mountPolicy) override;

  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override;

  std::optional<common::dataStructures::MountPolicy> getMountPolicy(const std::string& mountPolicyName) const override;

  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override;

  void deleteMountPolicy(const std::string& name) override;

  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin,
                                        const std::string& name,
                                        const uint64_t archivePriority) override;

  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin,
                                             const std::string& name,
                                             const uint64_t minArchiveRequestAge) override;

  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin,
                                         const std::string& name,
                                         const uint64_t retrievePriority) override;

  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& name,
                                              const uint64_t minRetrieveRequestAge) override;

  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin,
                                const std::string& name,
                                const std::string& comment) override;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  friend class RdbmsArchiveFileCatalogue;
  friend class RdbmsRequesterMountRuleCatalogue;
  friend class RdbmsRequesterGroupMountRuleCatalogue;

  std::optional<common::dataStructures::MountPolicy> getMountPolicy(rdbms::Conn& conn,
                                                                    const std::string& mountPolicyName) const;

  std::list<common::dataStructures::MountPolicy> getMountPolicies(rdbms::Conn& conn) const;

  /**
   * Returns the specified requester mount-policy or std::nullopt if one does not
   * exist.
   *
   * @param conn The database connection.
   * @param user The fully qualified user, in other words the name of the disk
   * instance and the name of the group.
   * @return The mount policy or std::nullopt if one does not exists.
   */
  std::optional<common::dataStructures::MountPolicy> getRequesterMountPolicy(rdbms::Conn& conn, const User& user) const;

  /**
   * Returns the specified requester-group mount-policy or nullptr if one does
   * not exist.
   *
   * @param conn The database connection.
   * @param group The fully qualified group, in other words the name of the disk
   * instance and the name of the group.
   * @return The mount policy or std::nullopt if one does not exists.
   */
  std::optional<common::dataStructures::MountPolicy> getRequesterGroupMountPolicy(rdbms::Conn& conn,
                                                                                  const Group& group) const;

  friend class RdbmsTapeFileCatalogue;
  /**
   * Returns the mount policies for the specified requester and requester group.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester and requester group belong.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   * @return The mount policies.
   */
  RequesterAndGroupMountPolicies getMountPolicies(rdbms::Conn& conn,
                                                  const std::string& diskInstanceName,
                                                  const std::string& requesterName,
                                                  const std::string& requesterGroupName) const;

  /**
   * Returns the mount policies for the specified requester, requester group and requester activity.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester and requester group belong.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   * @param activity The name of the activity to match the requester activity
   * mount rules against
   * @return The mount policies.
   */
  RequesterAndGroupMountPolicies getMountPolicies(rdbms::Conn& conn,
                                                  const std::string& diskInstanceName,
                                                  const std::string& requesterName,
                                                  const std::string& requesterGroupName,
                                                  const std::string& activity) const;
};

}  // namespace catalogue
}  // namespace cta