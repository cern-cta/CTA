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

#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsDiskInstanceCatalogue.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsDiskInstanceCatalogue::RdbmsDiskInstanceCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool)
  : m_log(log), m_connPool(connPool) {}

void RdbmsDiskInstanceCatalogue::createDiskInstance(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create disk system because the name is an empty string");
  }
  if(comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot create disk system because the comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

  auto conn = m_connPool->getConn();
  if(RdbmsCatalogueUtils::diskInstanceExists(conn, name)) {
    throw exception::UserError(std::string("Cannot create disk instance ") + name +
      " because a disk instance with the same name identifier already exists");
  }

  const time_t now = time(nullptr);
  const char *const sql =
    "INSERT INTO DISK_INSTANCE("
      "DISK_INSTANCE_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":DISK_INSTANCE_NAME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":LAST_UPDATE_USER_NAME,"
      ":LAST_UPDATE_HOST_NAME,"
      ":LAST_UPDATE_TIME)";
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":DISK_INSTANCE_NAME", name);

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();
}

void RdbmsDiskInstanceCatalogue::deleteDiskInstance(const std::string &name) {
  const char *const delete_sql =
    "DELETE "
    "FROM "
      "DISK_INSTANCE "
    "WHERE "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DISK_INSTANCE_NAME", name);
  stmt.executeNonQuery();

  // The delete statement will effect no rows and will not raise an error if
  // either the tape does not exist or if it still has tape files
  if(0 == stmt.getNbAffectedRows()) {
    if(RdbmsCatalogueUtils::diskInstanceExists(conn, name)) {
      throw UserSpecifiedANonEmptyDiskInstanceAfterDelete(std::string("Cannot delete disk instance ") + name + " for unknown reason");
    } else {
      throw UserSpecifiedANonExistentDiskInstance(std::string("Cannot delete disk instance ") + name + " because it does not exist");
    }
  }
}

void RdbmsDiskInstanceCatalogue::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot modify disk instance"
      " because the disk instance name is an empty string");
  }
  if(comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot modify disk instance "
      "because the new comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

  const time_t now = time(nullptr);
  const char *const sql =
    "UPDATE DISK_INSTANCE SET "
      "USER_COMMENT = :USER_COMMENT,"
      "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
    "WHERE "
      "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw UserSpecifiedANonExistentDiskInstance(std::string("Cannot modify disk instance ") + name + " because it does not exist");
  }
}

std::list<common::dataStructures::DiskInstance> RdbmsDiskInstanceCatalogue::getAllDiskInstances() const {
  std::list<common::dataStructures::DiskInstance> diskInstanceList;
  std::string sql =
    "SELECT "
      "DISK_INSTANCE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"

      "DISK_INSTANCE.USER_COMMENT AS USER_COMMENT,"

      "DISK_INSTANCE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "DISK_INSTANCE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "DISK_INSTANCE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

      "DISK_INSTANCE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "DISK_INSTANCE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "DISK_INSTANCE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "DISK_INSTANCE";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);

  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::DiskInstance diskInstance;
    diskInstance.name = rset.columnString("DISK_INSTANCE_NAME");
    diskInstance.comment = rset.columnString("USER_COMMENT");
    diskInstance.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    diskInstance.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    diskInstance.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    diskInstance.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    diskInstance.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    diskInstance.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
    diskInstanceList.push_back(diskInstance);
  }
  return diskInstanceList;
}

} // namespace cta::catalogue
