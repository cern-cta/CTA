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
#include "catalogue/rdbms/RdbmsRequesterActivityMountRuleCatalogue.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsRequesterActivityMountRuleCatalogue::RdbmsRequesterActivityMountRuleCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue):
  m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRulePolicy(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) {
  const time_t now = time(nullptr);
  const char *const sql =
    "UPDATE REQUESTER_ACTIVITY_MOUNT_RULE SET "
      "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
      "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
    "WHERE "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
      "REQUESTER_NAME = :REQUESTER_NAME AND "
      "ACTIVITY_REGEX = :ACTIVITY_REGEX";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  stmt.bindString(":ACTIVITY_REGEX", activityRegex);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify requester  activity mount rule ") + instanceName + ":" +
      requesterName + "for activities matching " + activityRegex + " because it does not exist");
  }
}

void RdbmsRequesterActivityMountRuleCatalogue::modifyRequesterActivityMountRuleComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName,
  const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
  const time_t now = time(nullptr);
  const char *const sql =
    "UPDATE REQUESTER_ACTIVITY_MOUNT_RULE SET "
      "USER_COMMENT = :USER_COMMENT,"
      "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
    "WHERE "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
      "REQUESTER_NAME = :REQUESTER_NAME AND "
      "ACTIVITY_REGEX = :ACTIVITY_REGEX";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  stmt.bindString(":ACTIVITY_REGEX", activityRegex);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify requester  activity mount rule ") + instanceName + ":" +
      requesterName + "for activities matching " + activityRegex + " because it does not exist");
  }
}

void RdbmsRequesterActivityMountRuleCatalogue::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName, const std::string &diskInstanceName, const std::string &requesterName,
  const std::string &activityRegex, const std::string &comment) {
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
  auto conn = m_connPool->getConn();
  if(RdbmsCatalogueUtils::requesterActivityMountRuleExists(conn, diskInstanceName, requesterName, activityRegex)) {
    throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
      " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
      " because that requester-activity mount rule already exists");
  }
  if(!RdbmsCatalogueUtils::mountPolicyExists(conn, mountPolicyName)) {
    throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
      " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
      " because mount-policy " + mountPolicyName + " does not exist");
  }
  if(!RdbmsCatalogueUtils::diskInstanceExists(conn, diskInstanceName)) {
    throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
      " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
      " because disk-instance " + diskInstanceName + " does not exist");
  }

  const uint64_t now = time(nullptr);
  const char *const sql =
    "INSERT INTO REQUESTER_ACTIVITY_MOUNT_RULE("
      "DISK_INSTANCE_NAME,"
      "REQUESTER_NAME,"
      "MOUNT_POLICY_NAME,"
      "ACTIVITY_REGEX,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":DISK_INSTANCE_NAME,"
      ":REQUESTER_NAME,"
      ":MOUNT_POLICY_NAME,"
      ":ACTIVITY_REGEX,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":LAST_UPDATE_USER_NAME,"
      ":LAST_UPDATE_HOST_NAME,"
      ":LAST_UPDATE_TIME)";
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
  stmt.bindString(":ACTIVITY_REGEX", activityRegex);

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();

  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
}

std::list<common::dataStructures::RequesterActivityMountRule>
  RdbmsRequesterActivityMountRuleCatalogue::getRequesterActivityMountRules() const {
  std::list<common::dataStructures::RequesterActivityMountRule> rules;
  const char *const sql =
    "SELECT "
      "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "REQUESTER_NAME AS REQUESTER_NAME,"
      "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
      "ACTIVITY_REGEX AS ACTIVITY_REGEX,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "REQUESTER_ACTIVITY_MOUNT_RULE "
    "ORDER BY "
      "DISK_INSTANCE_NAME, REQUESTER_NAME, ACTIVITY_REGEX, MOUNT_POLICY_NAME";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  while(rset.next()) {
    common::dataStructures::RequesterActivityMountRule rule;

    rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    rule.name = rset.columnString("REQUESTER_NAME");
    rule.mountPolicy = rset.columnString("MOUNT_POLICY_NAME");
    rule.activityRegex = rset.columnString("ACTIVITY_REGEX");
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

void RdbmsRequesterActivityMountRuleCatalogue::deleteRequesterActivityMountRule(const std::string &diskInstanceName,
  const std::string &requesterName, const std::string &activityRegex) {
  const char *const sql =
    "DELETE FROM "
      "REQUESTER_ACTIVITY_MOUNT_RULE "
    "WHERE "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
      "REQUESTER_NAME = :REQUESTER_NAME AND "
      "ACTIVITY_REGEX = :ACTIVITY_REGEX";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  stmt.bindString(":ACTIVITY_REGEX", activityRegex);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":"
      + requesterName + " and activity regex " + activityRegex + " because the rule does not exist");
  }

  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
}

} // namespace cta::catalogue
