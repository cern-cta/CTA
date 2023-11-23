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

#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsMountPolicyCatalogue.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"
#include "common/utils/Regex.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsMountPolicyCatalogue::RdbmsMountPolicyCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue):
  m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsMountPolicyCatalogue::createMountPolicy(const common::dataStructures::SecurityIdentity &admin,
  const CreateMountPolicyAttributes & mountPolicy) {
  std::string name = mountPolicy.name;
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(mountPolicy.comment, &m_log);
    auto conn = m_connPool->getConn();
    if (RdbmsCatalogueUtils::mountPolicyExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create mount policy ") + name +
        " because a mount policy with the same name already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO MOUNT_POLICY("
        "MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":MOUNT_POLICY_NAME,"

        ":ARCHIVE_PRIORITY,"
        ":ARCHIVE_MIN_REQUEST_AGE,"

        ":RETRIEVE_PRIORITY,"
        ":RETRIEVE_MIN_REQUEST_AGE,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":MOUNT_POLICY_NAME", name);

    stmt.bindUint64(":ARCHIVE_PRIORITY", mountPolicy.archivePriority);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", mountPolicy.minArchiveRequestAge);

    stmt.bindUint64(":RETRIEVE_PRIORITY", mountPolicy.retrievePriority);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", mountPolicy.minRetrieveRequestAge);

    stmt.bindString(":USER_COMMENT", trimmedComment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

void RdbmsMountPolicyCatalogue::deleteMountPolicy(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM MOUNT_POLICY WHERE MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount policy ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

std::list<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getMountPolicies() const {
  try {
    auto conn = m_connPool->getConn();
    return getMountPolicies(conn);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getMountPolicies(rdbms::Conn & conn) const {
try {
    std::list<common::dataStructures::MountPolicy> policies;
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "ORDER BY "
        "MOUNT_POLICY_NAME";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");

      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      policies.push_back(policy);
    }
    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getMountPolicy(const std::string &mountPolicyName) const {
  try {
    auto conn = m_connPool->getConn();
    return getMountPolicy(conn, mountPolicyName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getMountPolicy(rdbms::Conn &conn, const std::string &mountPolicyName) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    auto rset = stmt.executeQuery();
    if (rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");

      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    }
    return std::nullopt;

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getCachedMountPolicies() const {
  try {
    auto l_getNonCachedValue = [this] {
      auto conn = m_connPool->getConn();
      return getMountPolicies(conn);
    };
    return m_rdbmsCatalogue->m_allMountPoliciesCache.getCachedValue("all",l_getNonCachedValue).value;
  } catch(exception::UserError&) {
    throw;
  } catch(exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsMountPolicyCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t archivePriority) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "ARCHIVE_PRIORITY = :ARCHIVE_PRIORITY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_PRIORITY", archivePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

void RdbmsMountPolicyCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t minArchiveRequestAge) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "ARCHIVE_MIN_REQUEST_AGE = :ARCHIVE_MIN_REQUEST_AGE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

void RdbmsMountPolicyCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t retrievePriority) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "RETRIEVE_PRIORITY = :RETRIEVE_PRIORITY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":RETRIEVE_PRIORITY", retrievePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

void RdbmsMountPolicyCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t minRetrieveRequestAge) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "RETRIEVE_MIN_REQUEST_AGE = :RETRIEVE_MIN_REQUEST_AGE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

void RdbmsMountPolicyCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_rdbmsCatalogue->m_groupMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_userMountPolicyCache.invalidate();
  m_rdbmsCatalogue->m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// getRequesterGroupMountPolicy
//------------------------------------------------------------------------------
std::optional<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getRequesterGroupMountPolicy(
  rdbms::Conn &conn, const Group &group) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"

        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "INNER JOIN "
        "REQUESTER_GROUP_MOUNT_RULE "
      "ON "
        "MOUNT_POLICY.MOUNT_POLICY_NAME = REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", group.diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", group.groupName);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    } else {
      return std::nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<common::dataStructures::MountPolicy> RdbmsMountPolicyCatalogue::getRequesterMountPolicy(rdbms::Conn &conn,
  const User &user) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"

        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "INNER JOIN "
        "REQUESTER_MOUNT_RULE "
      "ON "
        "MOUNT_POLICY.MOUNT_POLICY_NAME = REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", user.diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", user.username);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");

      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      common::dataStructures::EntryLog updateLog;
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    } else {
      return std::nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

RequesterAndGroupMountPolicies RdbmsMountPolicyCatalogue::getMountPolicies(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &requesterGroupName,
  const std::string &activity) const {
  try {
    const char *const sql =
      "SELECT "
        "'ACTIVITY' AS RULE_TYPE,"
        "REQUESTER_ACTIVITY_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"
        "REQUESTER_ACTIVITY_MOUNT_RULE.ACTIVITY_REGEX AS ACTIVITY_REGEX,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_ACTIVITY_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_ACTIVITY_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_ACTIVITY_MOUNT_RULE.DISK_INSTANCE_NAME = :ACTIVITY_DISK_INSTANCE_NAME AND "
        "REQUESTER_ACTIVITY_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_ACTIVITY_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER' AS RULE_TYPE,"
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"
        "'' AS ACTIVITY_REGEX,"


        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :REQUESTER_DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER_GROUP' AS RULE_TYPE,"
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME AS ASSIGNEE,"
        "'' AS ACTIVITY_REGEX,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :GROUP_DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ACTIVITY_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":GROUP_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_ACTIVITY_NAME", requesterName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();

    RequesterAndGroupMountPolicies policies;
    while(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");
      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");
      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");
      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      if(rset.columnString("RULE_TYPE") == "ACTIVITY") {
        auto activityRegexString = rset.columnString("ACTIVITY_REGEX");
        cta::utils::Regex activityRegex(activityRegexString);
        if (activityRegex.has_match(activity)) {
          policies.requesterActivityMountPolicies.push_back(policy);
        }
      } else if(rset.columnString("RULE_TYPE") == "REQUESTER") {
        policies.requesterMountPolicies.push_back(policy);
      } else {
        policies.requesterGroupMountPolicies.push_back(policy);
      }
    }

    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

RequesterAndGroupMountPolicies RdbmsMountPolicyCatalogue::getMountPolicies(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &requesterGroupName) const {
  try {
    const char *const sql =
      "SELECT "
        "'REQUESTER' AS RULE_TYPE,"
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :REQUESTER_DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER_GROUP' AS RULE_TYPE,"
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME AS ASSIGNEE,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :GROUP_DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":REQUESTER_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":GROUP_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();

    RequesterAndGroupMountPolicies policies;
    while(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");
      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");
      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");
      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      if(rset.columnString("RULE_TYPE") == "REQUESTER") {
        policies.requesterMountPolicies.push_back(policy);
      } else {
        policies.requesterGroupMountPolicies.push_back(policy);
      }
    }

    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace cta::catalogue
