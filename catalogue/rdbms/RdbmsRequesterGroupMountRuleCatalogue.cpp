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

#include <string>

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsMountPolicyCatalogue.hpp"
#include "catalogue/rdbms/RdbmsRequesterGroupMountRuleCatalogue.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsRequesterGroupMountRuleCatalogue::RdbmsRequesterGroupMountRuleCatalogue(log::Logger& log,
                                                                             std::shared_ptr<rdbms::ConnPool> connPool,
                                                                             RdbmsCatalogue* rdbmsCatalogue) :
m_log(log),
m_connPool(connPool),
m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRulePolicy(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char* const sql = "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
                            "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
                            "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
                            "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
                            "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
                            "WHERE "
                            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
                            "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
                                 requesterGroupName + " because it does not exist");
    }
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsRequesterGroupMountRuleCatalogue::modifyRequesterGroupMountRuleComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& instanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    const time_t now = time(nullptr);
    const char* const sql = "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
                            "USER_COMMENT = :USER_COMMENT,"
                            "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
                            "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
                            "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
                            "WHERE "
                            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
                            "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
                                 requesterGroupName + " because it does not exist");
    }
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsRequesterGroupMountRuleCatalogue::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& mountPolicyName,
  const std::string& diskInstanceName,
  const std::string& requesterGroupName,
  const std::string& comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    auto conn = m_connPool->getConn();
    {
      const auto group = Group(diskInstanceName, requesterGroupName);
      const auto mountPolicyCatalogue = static_cast<RdbmsMountPolicyCatalogue*>(m_rdbmsCatalogue->MountPolicy().get());
      const auto mountPolicy = mountPolicyCatalogue->getRequesterGroupMountPolicy(conn, group);
      if (mountPolicy) {
        throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
                                   " to requester-group " + diskInstanceName + ":" + requesterGroupName +
                                   " because a rule already exists assigning the requester-group to mount-policy " +
                                   mountPolicy->name);
      }
    }
    if (!RdbmsCatalogueUtils::mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot assign mount-policy ") + mountPolicyName + " to requester-group " +
                                 diskInstanceName + ":" + requesterGroupName + " because mount-policy " +
                                 mountPolicyName + " does not exist");
    }
    if (!RdbmsCatalogueUtils::diskInstanceExists(conn, diskInstanceName)) {
      throw exception::UserError(std::string("Cannot assign mount-policy ") + mountPolicyName + " to requester-group " +
                                 diskInstanceName + ":" + requesterGroupName + " because disk-instance " +
                                 diskInstanceName + " does not exist");
    }

    const uint64_t now = time(nullptr);
    const char* const sql = "INSERT INTO REQUESTER_GROUP_MOUNT_RULE("
                            "DISK_INSTANCE_NAME,"
                            "REQUESTER_GROUP_NAME,"
                            "MOUNT_POLICY_NAME,"

                            "USER_COMMENT,"

                            "CREATION_LOG_USER_NAME,"
                            "CREATION_LOG_HOST_NAME,"
                            "CREATION_LOG_TIME,"

                            "LAST_UPDATE_USER_NAME,"
                            "LAST_UPDATE_HOST_NAME,"
                            "LAST_UPDATE_TIME)"
                            "VALUES("
                            ":DISK_INSTANCE_NAME,"
                            ":REQUESTER_GROUP_NAME,"
                            ":MOUNT_POLICY_NAME,"

                            ":USER_COMMENT,"

                            ":CREATION_LOG_USER_NAME,"
                            ":CREATION_LOG_HOST_NAME,"
                            ":CREATION_LOG_TIME,"

                            ":LAST_UPDATE_USER_NAME,"
                            ":LAST_UPDATE_HOST_NAME,"
                            ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt.bindString(":USER_COMMENT", trimmedComment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
}

std::list<common::dataStructures::RequesterGroupMountRule>
  RdbmsRequesterGroupMountRuleCatalogue::getRequesterGroupMountRules() const {
  try {
    std::list<common::dataStructures::RequesterGroupMountRule> rules;
    const char* const sql = "SELECT "
                            "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
                            "REQUESTER_GROUP_NAME AS REQUESTER_GROUP_NAME,"
                            "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

                            "USER_COMMENT AS USER_COMMENT,"

                            "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
                            "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
                            "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

                            "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
                            "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
                            "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
                            "FROM "
                            "REQUESTER_GROUP_MOUNT_RULE "
                            "ORDER BY "
                            "DISK_INSTANCE_NAME, REQUESTER_GROUP_NAME, MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::RequesterGroupMountRule rule;

      rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      rule.name = rset.columnString("REQUESTER_GROUP_NAME");
      rule.mountPolicy = rset.columnString("MOUNT_POLICY_NAME");

      rule.comment = rset.columnString("USER_COMMENT");
      rule.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsRequesterGroupMountRuleCatalogue::deleteRequesterGroupMountRule(const std::string& diskInstanceName,
                                                                          const std::string& requesterGroupName) {
  try {
    const char* const sql = "DELETE FROM "
                            "REQUESTER_GROUP_MOUNT_RULE "
                            "WHERE "
                            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
                            "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete the mount rule for requester group ") + diskInstanceName +
                                 ":" + requesterGroupName + " because it does not exist");
    }
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
}

}  // namespace catalogue
}  // namespace cta