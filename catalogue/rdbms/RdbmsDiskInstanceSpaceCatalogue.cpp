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
#include <memory>

#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsDiskInstanceCatalogue.hpp"
#include "catalogue/rdbms/RdbmsDiskInstanceSpaceCatalogue.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsDiskInstanceSpaceCatalogue::RdbmsDiskInstanceSpaceCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool)
  : m_log(log), m_connPool(connPool) {}

void RdbmsDiskInstanceSpaceCatalogue::deleteDiskInstanceSpace(const std::string &name,
  const std::string &diskInstance) {
  const char* const delete_sql = R"SQL(
    DELETE 
    FROM 
      DISK_INSTANCE_SPACE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
    AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(delete_sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.executeNonQuery();

  // The delete statement will effect no rows and will not raise an error if
  // either the tape does not exist or if it still has tape files
  if(0 == stmt.getNbAffectedRows()) {
    if(diskInstanceSpaceExists(conn, name, diskInstance)) {
      throw UserSpecifiedANonEmptyDiskInstanceSpaceAfterDelete(std::string("Cannot delete disk instance space")
        + name + " for unknown reason");
    } else {
      throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot delete disk instance space ") 
        + name + " because it does not exist");
    }
  }
}

void RdbmsDiskInstanceSpaceCatalogue::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL,
  const uint64_t refreshInterval, const std::string &comment) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot create disk instance space because the name is an empty string");
  }
  if(freeSpaceQueryURL.empty()) {
    throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot create disk instance space because the free space query URL is an empty string");
  }
  if(0 == refreshInterval) {
    throw UserSpecifiedAZeroRefreshInterval("Cannot create disk instance space because the refresh interval is zero");
  }
  if(comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot create disk instance space because the comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

  auto conn = m_connPool->getConn();
  if(!RdbmsCatalogueUtils::diskInstanceExists(conn, diskInstance)) {
    throw exception::UserError(std::string("Cannot create disk instance space ") + name +
      " for disk instance " + diskInstance +
      " because the disk instance does not exist");
  }

  if (diskInstanceSpaceExists(conn, name, diskInstance)) {
    throw exception::UserError(std::string("Cannot create disk instance space ") + name +
      " for disk instance " + diskInstance +
      " because a disk instance space with the same name and disk instance already exists");
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    INSERT INTO DISK_INSTANCE_SPACE(
      DISK_INSTANCE_NAME,
      DISK_INSTANCE_SPACE_NAME,
      FREE_SPACE_QUERY_URL,
      REFRESH_INTERVAL,
      LAST_REFRESH_TIME,
      FREE_SPACE,

      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME)
    VALUES(
      :DISK_INSTANCE_NAME,
      :DISK_INSTANCE_SPACE_NAME,
      :FREE_SPACE_QUERY_URL,
      :REFRESH_INTERVAL,
      :LAST_REFRESH_TIME,
      :FREE_SPACE,

      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME)
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
  stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
  stmt.bindUint64(":LAST_REFRESH_TIME", 0);
  stmt.bindUint64(":FREE_SPACE", 0);

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();
}

std::list<common::dataStructures::DiskInstanceSpace> RdbmsDiskInstanceSpaceCatalogue::getAllDiskInstanceSpaces() const {
  std::list<common::dataStructures::DiskInstanceSpace> diskInstanceSpaceList;
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_SPACE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,
      DISK_INSTANCE_SPACE.DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME,
      DISK_INSTANCE_SPACE.FREE_SPACE_QUERY_URL AS FREE_SPACE_QUERY_URL,
      DISK_INSTANCE_SPACE.REFRESH_INTERVAL AS REFRESH_INTERVAL,
      DISK_INSTANCE_SPACE.LAST_REFRESH_TIME AS LAST_REFRESH_TIME,
      DISK_INSTANCE_SPACE.FREE_SPACE AS FREE_SPACE,

      DISK_INSTANCE_SPACE.USER_COMMENT AS USER_COMMENT,

      DISK_INSTANCE_SPACE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      DISK_INSTANCE_SPACE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      DISK_INSTANCE_SPACE.CREATION_LOG_TIME AS CREATION_LOG_TIME,

      DISK_INSTANCE_SPACE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      DISK_INSTANCE_SPACE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      DISK_INSTANCE_SPACE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      DISK_INSTANCE_SPACE
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);

  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::DiskInstanceSpace diskInstanceSpace;
    diskInstanceSpace.name = rset.columnString("DISK_INSTANCE_SPACE_NAME");
    diskInstanceSpace.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    diskInstanceSpace.freeSpaceQueryURL = rset.columnString("FREE_SPACE_QUERY_URL");
    diskInstanceSpace.refreshInterval =  rset.columnUint64("REFRESH_INTERVAL");
    diskInstanceSpace.freeSpace =  rset.columnUint64("FREE_SPACE");
    diskInstanceSpace.lastRefreshTime =  rset.columnUint64("LAST_REFRESH_TIME");
    diskInstanceSpace.comment = rset.columnString("USER_COMMENT");
    diskInstanceSpace.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    diskInstanceSpace.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    diskInstanceSpace.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    diskInstanceSpace.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    diskInstanceSpace.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    diskInstanceSpace.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
    diskInstanceSpaceList.push_back(diskInstanceSpace);
  }
  return diskInstanceSpaceList;
}

void RdbmsDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &comment) {

  if(comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot modify disk instance space "
      "because the new comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE DISK_INSTANCE_SPACE SET 
      USER_COMMENT = :USER_COMMENT,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
    AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name
      + " because it does not exist");
  }
}

void RdbmsDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceRefreshInterval(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const uint64_t refreshInterval) {
  if(0 == refreshInterval) {
    throw UserSpecifiedAZeroRefreshInterval("Cannot modify disk instance space "
      "because the new refreshInterval is zero");
  }
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE DISK_INSTANCE_SPACE SET 
      REFRESH_INTERVAL = :REFRESH_INTERVAL,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
    AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name
      + " because it does not exist");
  }
}

void RdbmsDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceFreeSpace(const std::string &name,
  const std::string &diskInstance, const uint64_t freeSpace) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE DISK_INSTANCE_SPACE SET 
      FREE_SPACE = :FREE_SPACE,
      LAST_REFRESH_TIME = :LAST_REFRESH_TIME 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
    AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":FREE_SPACE", freeSpace);
  stmt.bindUint64(":LAST_REFRESH_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name
      + " because it does not exist");
  }
}

void RdbmsDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceQueryURL(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &freeSpaceQueryURL) {
  if(freeSpaceQueryURL.empty()) {
    throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot modify disk instance space "
      "because the new freeSpaceQueryURL is an empty string");
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE DISK_INSTANCE_SPACE SET 
      FREE_SPACE_QUERY_URL = :FREE_SPACE_QUERY_URL,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
    AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name
      + " because it does not exist");
  }
}

bool RdbmsDiskInstanceSpaceCatalogue::diskInstanceSpaceExists(rdbms::Conn &conn, const std::string &name,
  const std::string &diskInstance) const {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME 
    FROM 
      DISK_INSTANCE_SPACE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME 
     AND 
      DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
  auto rset = stmt.executeQuery();
  return rset.next();
}

} // namespace cta::catalogue
