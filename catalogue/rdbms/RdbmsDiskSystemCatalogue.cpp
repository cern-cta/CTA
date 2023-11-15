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

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsDiskSystemCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "disk/DiskSystem.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsDiskSystemCatalogue::RdbmsDiskSystemCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool)
  : m_log(log), m_connPool(connPool) {}

void RdbmsDiskSystemCatalogue::createDiskSystem(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName,
  const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot create disk system because the name is an empty string");
    }
    if(fileRegexp.empty()) {
      throw UserSpecifiedAnEmptyStringFileRegexp("Cannot create disk system because the file regexp is an empty string");
    }
    if(diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create disk system because the disk instance name is an empty string");
    }
    if(diskInstanceSpaceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot create disk system because the disk instance space name is an empty string");
    }
    if(0 == targetedFreeSpace) {
      throw UserSpecifiedAZeroTargetedFreeSpace("Cannot create disk system because the targeted free space is zero");
    }
    if (0 == sleepTime) {
      throw UserSpecifiedAZeroSleepTime("Cannot create disk system because the sleep time is zero");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create disk system because the comment is an empty string");
    }
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::diskSystemExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create disk system ") + name +
        " because a disk system with the same name identifier already exists");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO DISK_SYSTEM("
        "DISK_SYSTEM_NAME,"
        "DISK_INSTANCE_NAME,"
        "DISK_INSTANCE_SPACE_NAME,"
        "FILE_REGEXP,"
        "TARGETED_FREE_SPACE,"
        "SLEEP_TIME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_SYSTEM_NAME,"
        ":DISK_INSTANCE_NAME,"
        ":DISK_INSTANCE_SPACE_NAME,"
        ":FILE_REGEXP,"
        ":TARGETED_FREE_SPACE,"
        ":SLEEP_TIME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

   stmt.bindString(":DISK_SYSTEM_NAME", name);
   stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
   stmt.bindString(":DISK_INSTANCE_SPACE_NAME", diskInstanceSpaceName);
   stmt.bindString(":FILE_REGEXP", fileRegexp);
   stmt.bindUint64(":TARGETED_FREE_SPACE", targetedFreeSpace);
   stmt.bindUint64(":SLEEP_TIME", sleepTime);

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
}

void RdbmsDiskSystemCatalogue::deleteDiskSystem(const std::string &name) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DISK_SYSTEM "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(delete_sql);
      stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(RdbmsCatalogueUtils::diskSystemExists(conn, name)) {
        throw UserSpecifiedANonEmptyDiskSystemAfterDelete(std::string("Cannot delete disk system ") + name + " for unknown reason");
      } else {
        throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot delete disk system ") + name + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

disk::DiskSystemList RdbmsDiskSystemCatalogue::getAllDiskSystems() const {
  try {
    disk::DiskSystemList diskSystemList;
    const std::string sql =
      "SELECT "
        "DISK_SYSTEM.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "DISK_SYSTEM.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "DISK_SYSTEM.DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME,"
        "DISK_SYSTEM.FILE_REGEXP AS FILE_REGEXP,"
        "DISK_SYSTEM.TARGETED_FREE_SPACE AS TARGETED_FREE_SPACE,"
        "DISK_SYSTEM.SLEEP_TIME AS SLEEP_TIME,"

        "DISK_SYSTEM.USER_COMMENT AS USER_COMMENT,"

        "DISK_SYSTEM.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DISK_SYSTEM.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DISK_SYSTEM.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_SYSTEM.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,"

        "DISK_INSTANCE_SPACE.FREE_SPACE_QUERY_URL AS FREE_SPACE_QUERY_URL,"
        "DISK_INSTANCE_SPACE.REFRESH_INTERVAL AS REFRESH_INTERVAL,"
        "DISK_INSTANCE_SPACE.LAST_REFRESH_TIME AS LAST_REFRESH_TIME,"
        "DISK_INSTANCE_SPACE.FREE_SPACE AS FREE_SPACE "
      "FROM "
        "DISK_SYSTEM "
      "INNER JOIN DISK_INSTANCE_SPACE ON "
        "DISK_SYSTEM.DISK_INSTANCE_NAME = DISK_INSTANCE_SPACE.DISK_INSTANCE_NAME "
      "AND "
        "DISK_SYSTEM.DISK_INSTANCE_SPACE_NAME = DISK_INSTANCE_SPACE.DISK_INSTANCE_SPACE_NAME"
      ;

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      disk::DiskSystem diskSystem;
      diskSystem.name = rset.columnString("DISK_SYSTEM_NAME");
      diskSystem.fileRegexp = rset.columnString("FILE_REGEXP");
      diskSystem.targetedFreeSpace =  rset.columnUint64("TARGETED_FREE_SPACE");
      diskSystem.sleepTime =  rset.columnUint64("SLEEP_TIME");
      diskSystem.comment = rset.columnString("USER_COMMENT");
      diskSystem.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      diskSystem.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      diskSystem.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      diskSystem.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      diskSystem.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      diskSystem.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
      diskSystem.diskInstanceSpace.freeSpaceQueryURL = rset.columnString("FREE_SPACE_QUERY_URL");
      diskSystem.diskInstanceSpace.refreshInterval = rset.columnUint64("REFRESH_INTERVAL");
      diskSystem.diskInstanceSpace.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      diskSystem.diskInstanceSpace.name = rset.columnString("DISK_INSTANCE_SPACE_NAME");
      diskSystem.diskInstanceSpace.lastRefreshTime = rset.columnUint64("LAST_REFRESH_TIME");
      diskSystem.diskInstanceSpace.freeSpace = rset.columnUint64("FREE_SPACE");
      diskSystemList.push_back(diskSystem);
    }
    return diskSystemList;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &fileRegexp) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(fileRegexp.empty()) {
      throw UserSpecifiedAnEmptyStringFileRegexp("Cannot modify disk system "
        "because the new fileRegexp is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "FILE_REGEXP = :FILE_REGEXP,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":FILE_REGEXP", fileRegexp);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t targetedFreeSpace) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(0 == targetedFreeSpace) {
      throw UserSpecifiedAZeroTargetedFreeSpace("Cannot modify disk system "
        "because the new targeted free space has zero value");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "TARGETED_FREE_SPACE = :TARGETED_FREE_SPACE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
        stmt.bindUint64(":TARGETED_FREE_SPACE", targetedFreeSpace);
        stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
        stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
        stmt.bindUint64(":LAST_UPDATE_TIME", now);
        stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify disk system "
        "because the new comment is an empty string");
    }
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
  const std::string& name, const uint64_t sleepTime) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(sleepTime == 0) {
      throw UserSpecifiedAZeroSleepTime("Cannot modify disk system "
        "because the new sleep time is zero");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "SLEEP_TIME = :SLEEP_TIME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":SLEEP_TIME", sleepTime);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceName) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot modify disk system "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsDiskSystemCatalogue::modifyDiskSystemDiskInstanceSpaceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const std::string &diskInstanceSpaceName) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(diskInstanceSpaceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot modify disk system "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", diskInstanceSpaceName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

bool RdbmsDiskSystemCatalogue::diskSystemExists(const std::string &name) const {
  try {
    auto conn = m_connPool->getConn();
    return RdbmsCatalogueUtils::diskSystemExists(conn, name);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace cta::catalogue
