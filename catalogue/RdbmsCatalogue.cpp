/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/RdbmsCatalogue.hpp"
#include "catalogue/RdbmsCatalogueGetArchiveFilesItor.hpp"
#include "catalogue/RdbmsCatalogueGetArchiveFilesForRepackItor.hpp"
#include "catalogue/retryOnLostConnection.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/UserSpecifiedANonExistentDiskSystem.hpp"
#include "catalogue/UserSpecifiedANonEmptyDiskSystemAfterDelete.hpp"
#include "catalogue/UserSpecifiedANonEmptyLogicalLibrary.hpp"
#include "catalogue/UserSpecifiedANonEmptyTape.hpp"
#include "catalogue/UserSpecifiedANonExistentLogicalLibrary.hpp"
#include "catalogue/UserSpecifiedANonExistentTape.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringComment.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringDiskSystemName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringFileRegexp.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringFreeSpaceQueryURL.hpp"
#include "catalogue/UserSpecifiedAZeroRefreshInterval.hpp"
#include "catalogue/UserSpecifiedAZeroTargetedFreeSpace.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringDiskInstanceName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringLogicalLibraryName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringMediaType.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringStorageClassName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringSupply.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringTapePoolName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringUsername.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVendor.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVid.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVo.hpp"
#include "catalogue/UserSpecifiedAnEmptyTapePool.hpp"
#include "catalogue/UserSpecifiedAZeroCapacity.hpp"
#include "catalogue/UserSpecifiedAZeroCopyNb.hpp"
#include "catalogue/UserSpecifiedStorageClassUsedByArchiveFiles.hpp"
#include "catalogue/UserSpecifiedStorageClassUsedByArchiveRoutes.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"

#include <ctype.h>
#include <memory>
#include <time.h>
#include <common/exception/LostDatabaseConnection.hpp>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::RdbmsCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  m_log(log),
  m_connPool(login, nbConns),
  m_archiveFileListingConnPool(login, nbArchiveFileListingConns),
  m_tapeCopyToPoolCache(10),
  m_groupMountPolicyCache(10),
  m_userMountPolicyCache(10),
  m_expectedNbArchiveRoutesCache(10),
  m_isAdminCache(10),
  m_activitiesFairShareWeights(10) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogue::~RdbmsCatalogue() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &username,
  const std::string &comment) {
  try {
    if(username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot create admin user because the username is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create admin user because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if (adminUserExists(conn, username)) {
      throw exception::UserError(std::string("Cannot create admin user " + username +
        " because an admin user with the same name already exists"));
    }
    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ADMIN_USER("
        "ADMIN_USER_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":ADMIN_USER_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":ADMIN_USER_NAME", username);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// adminUserExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::adminUserExists(rdbms::Conn &conn, const std::string adminUsername) const {
  try {
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME "
      "FROM "
        "ADMIN_USER "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", adminUsername);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteAdminUser(const std::string &username) {
  try {
    const char *const sql = "DELETE FROM ADMIN_USER WHERE ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-user ") + username + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<common::dataStructures::AdminUser> RdbmsCatalogue::getAdminUsers() const {
  try {
    std::list<common::dataStructures::AdminUser> admins;
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ADMIN_USER "
      "ORDER BY "
        "ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::AdminUser admin;

      admin.name = rset.columnString("ADMIN_USER_NAME");
      admin.comment = rset.columnString("USER_COMMENT");
      admin.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      admin.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      admin.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      admin.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      admin.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      admin.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      admins.push_back(admin);
    }

    return admins;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  try {
    if(username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot modify admin user because the username is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify admin user because the comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ADMIN_USER SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify admin user ") + username + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::StorageClass &storageClass) {
  try {
    if(storageClass.diskInstance.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create storage class because the disk instance name is"
        " an empty string");
    }

    if(storageClass.name.empty()) {
      throw UserSpecifiedAnEmptyStringStorageClassName("Cannot create storage class because the storage class name is"
        " an empty string");
    }

    if(storageClass.comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create storage class because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(storageClassExists(conn, storageClass.diskInstance, storageClass.name)) {
      throw exception::UserError(std::string("Cannot create storage class ") + storageClass.diskInstance + ":" +
        storageClass.name + " because it already exists");
    }
    const uint64_t storageClassId = getNextStorageClassId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO STORAGE_CLASS("
        "STORAGE_CLASS_ID,"
        "DISK_INSTANCE_NAME,"
        "STORAGE_CLASS_NAME,"
        "NB_COPIES,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":STORAGE_CLASS_ID,"
        ":DISK_INSTANCE_NAME,"
        ":STORAGE_CLASS_NAME,"
        ":NB_COPIES,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":STORAGE_CLASS_ID", storageClassId);
    stmt.bindString(":DISK_INSTANCE_NAME", storageClass.diskInstance);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.name);
    stmt.bindUint64(":NB_COPIES", storageClass.nbCopies);

    stmt.bindString(":USER_COMMENT", storageClass.comment);

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

//------------------------------------------------------------------------------
// storageClassExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteStorageClass(const std::string &diskInstanceName, const std::string &storageClassName) {
  try {
    auto conn = m_connPool.getConn();

    if(storageClassIsUsedByArchiveRoutes(conn, storageClassName)) {
      throw UserSpecifiedStorageClassUsedByArchiveRoutes(std::string("The ") + storageClassName +
        " storage class is being used by one or more archive routes");
    }

    if(storageClassIsUsedByArchiveFiles(conn, storageClassName)) {
      throw UserSpecifiedStorageClassUsedByArchiveFiles(std::string("The ") + storageClassName +
        " storage class is being used by one or more archive files");
    }

    const char *const sql =
      "DELETE FROM "
        "STORAGE_CLASS "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);

    stmt.executeNonQuery();
    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete storage-class ") + diskInstanceName + ":" +
        storageClassName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassIsUsedByArchiveRoutes
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassIsUsedByArchiveRoutes(rdbms::Conn &conn, const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassIsUsedByARchiveFiles
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassIsUsedByArchiveFiles(rdbms::Conn &conn, const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<common::dataStructures::StorageClass> RdbmsCatalogue::getStorageClasses() const {
  try {
    std::list<common::dataStructures::StorageClass> storageClasses;
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "NB_COPIES AS NB_COPIES,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "STORAGE_CLASS "
      "ORDER BY "
        "DISK_INSTANCE_NAME, STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::StorageClass storageClass;

      storageClass.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      storageClass.name = rset.columnString("STORAGE_CLASS_NAME");
      storageClass.nbCopies = rset.columnUint64("NB_COPIES");
      storageClass.comment = rset.columnString("USER_COMMENT");
      storageClass.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      storageClass.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      storageClass.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      storageClass.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      storageClass.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      storageClass.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      storageClasses.push_back(storageClass);
    }

    return storageClasses;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &name, const uint64_t nbCopies) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "NB_COPIES = :NB_COPIES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":NB_COPIES", nbCopies);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class ") + instanceName + ":" + name +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class ") + instanceName + ":" + name +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &currentName, const std::string &newName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "STORAGE_CLASS_NAME = :NEW_STORAGE_CLASS_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :CURRENT_STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_STORAGE_CLASS_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":CURRENT_STORAGE_CLASS_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class ") + instanceName + ":" + currentName +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTapePool(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &vo,
  const uint64_t nbPartialTapes,
  const bool encryptionValue,
  const cta::optional<std::string> &supply,
  const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape pool because the tape pool name is an empty string");
    }

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot create tape pool because the VO is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create tape pool because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();

    if(tapePoolExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create tape pool ") + name +
        " because a tape pool with the same name already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE_POOL("
        "TAPE_POOL_NAME,"
        "VO,"
        "NB_PARTIAL_TAPES,"
        "IS_ENCRYPTED,"
        "SUPPLY,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":TAPE_POOL_NAME,"
        ":VO,"
        ":NB_PARTIAL_TAPES,"
        ":IS_ENCRYPTED,"
        ":SUPPLY,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.bindString(":VO", vo);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
    stmt.bindOptionalString(":SUPPLY", supply);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// tapePoolExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolExists(const std::string &tapePoolName) const {
  try {
    auto conn = m_connPool.getConn();
    return tapePoolExists(conn, tapePoolName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapePoolExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolExists(rdbms::Conn &conn, const std::string &tapePoolName) const {
  try {
    const char *const sql =
      "SELECT "
        "TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "TAPE_POOL "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// archiveFileExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::archiveFileIdExists(rdbms::Conn &conn, const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileIdExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileIdExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_ID AS DISK_FILE_ID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_ID = :DISK_FILE_ID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFilePathExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFilePathExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFilePath) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_PATH AS DISK_FILE_PATH "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_PATH = :DISK_FILE_PATH";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_PATH", diskFilePath);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileUserExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileUserExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileOwnerUid) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_UID AS DISK_FILE_UID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_UID = :DISK_FILE_UID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindUint64(":DISK_FILE_UID", diskFileOwnerUid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileGroupExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileGid) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_GID AS DISK_FILE_GID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_GID = :DISK_FILE_GID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindUint64(":DISK_FILE_GID", diskFileGid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// archiveRouteExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::archiveRouteExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &storageClassName, const uint32_t copyNb) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID AS STORAGE_CLASS_ID,"
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "ARCHIVE_ROUTE.COPY_NB = :COPY_NB";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapePool(const std::string &name) {
  try {
    auto conn = m_connPool.getConn();
    const uint64_t nbTapesInPool = getNbTapesInPool(conn, name);

    if(0 == nbTapesInPool) {
      const char *const sql = "DELETE FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME";
      auto stmt = conn.createStmt(sql);
      stmt.bindString(":TAPE_POOL_NAME", name);
      stmt.executeNonQuery();

      if(0 == stmt.getNbAffectedRows()) {
        throw exception::UserError(std::string("Cannot delete tape-pool ") + name + " because it does not exist");
      }
    } else {
      throw UserSpecifiedAnEmptyTapePool(std::string("Cannot delete tape-pool ") + name + " because it is not empty");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<TapePool> RdbmsCatalogue::getTapePools() const {
  try {
    std::list<TapePool> pools;
    const char *const sql =
      "SELECT "
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "COALESCE(TAPE_POOL.VO, 'NONE') AS VO," // TBD Remove COALESCE
        "TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,"
        "TAPE_POOL.SUPPLY AS SUPPLY,"

        "COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,"
        "COALESCE(SUM(TAPE.CAPACITY_IN_BYTES), 0) AS CAPACITY_IN_BYTES,"
        "COALESCE(SUM(TAPE.DATA_IN_BYTES), 0) AS DATA_IN_BYTES,"
        "COALESCE(SUM(TAPE.LAST_FSEQ), 0) AS NB_PHYSICAL_FILES,"

        "TAPE_POOL.USER_COMMENT AS USER_COMMENT,"

        "TAPE_POOL.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE_POOL.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE_POOL.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE_POOL.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE_POOL.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE_POOL.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE_POOL "
      "LEFT OUTER JOIN TAPE ON "
        "TAPE_POOL.TAPE_POOL_NAME = TAPE.TAPE_POOL_NAME "
      "GROUP BY "
        "TAPE_POOL.TAPE_POOL_NAME,"
        "TAPE_POOL.VO,"
        "TAPE_POOL.NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED,"
        "TAPE_POOL.SUPPLY,"
        "TAPE_POOL.USER_COMMENT,"
        "TAPE_POOL.CREATION_LOG_USER_NAME,"
        "TAPE_POOL.CREATION_LOG_HOST_NAME,"
        "TAPE_POOL.CREATION_LOG_TIME,"
        "TAPE_POOL.LAST_UPDATE_USER_NAME,"
        "TAPE_POOL.LAST_UPDATE_HOST_NAME,"
        "TAPE_POOL.LAST_UPDATE_TIME "
      "ORDER BY "
        "TAPE_POOL_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapePool pool;

      pool.name = rset.columnString("TAPE_POOL_NAME");
      pool.vo = rset.columnString("VO");
      pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
      pool.encryption = rset.columnBool("IS_ENCRYPTED");
      pool.supply = rset.columnOptionalString("SUPPLY");
      pool.nbTapes = rset.columnUint64("NB_TAPES");
      pool.capacityBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      pool.dataBytes = rset.columnUint64("DATA_IN_BYTES");
      pool.nbPhysicalFiles = rset.columnUint64("NB_PHYSICAL_FILES");
      pool.comment = rset.columnString("USER_COMMENT");
      pool.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      pool.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      pool.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      pool.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      pool.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      pool.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      pools.push_back(pool);
    }

    return pools;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolVO
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot modify tape pool because the new VO is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "VO = :VO,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VO", vo);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "NB_PARTIAL_TAPES = :NB_PARTIAL_TAPES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify tape pool because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool encryptionValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "IS_ENCRYPTED = :IS_ENCRYPTED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolSupply
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &supply) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    optional<std::string> optionalSupply;
    if(!supply.empty()) {
      optionalSupply = supply;
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "SUPPLY = :SUPPLY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindOptionalString(":SUPPLY", optionalSupply);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::createArchiveRoute(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &diskInstanceName,
  const std::string &storageClassName,
  const uint32_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  try {
    if(diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create archive route because disk instance name is an"
        " empty string");
    }
    if(storageClassName.empty()) {
      throw UserSpecifiedAnEmptyStringStorageClassName("Cannot create archive route because storage class name is an"
        " empty string");
    }
    if(0 == copyNb) {
      throw UserSpecifiedAZeroCopyNb("Cannot create archive route because copy number is zero");
    }
    if(tapePoolName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create archive route because tape pool name is an empty"
        " string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create archive route because comment is an empty string");
    }

    const time_t now = time(nullptr);
    auto conn = m_connPool.getConn();
    if(archiveRouteExists(conn, diskInstanceName, storageClassName, copyNb)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << diskInstanceName << ":" << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because it already exists";
      throw ue;
    }
    if(!storageClassExists(conn, diskInstanceName, storageClassName)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << diskInstanceName << ":" << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because storage class " << diskInstanceName << ":" << storageClassName <<
        " does not exist";
      throw ue;
    }
    if(!tapePoolExists(conn, tapePoolName)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << diskInstanceName << ":" << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because tape pool " << tapePoolName + " does not exist";
      throw ue;
    }

    const char *const sql =
      "INSERT INTO ARCHIVE_ROUTE("
        "STORAGE_CLASS_ID,"
        "COPY_NB,"
        "TAPE_POOL_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "SELECT "
        "STORAGE_CLASS_ID,"
        ":COPY_NB,"
        ":TAPE_POOL_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteArchiveRoute(const std::string &diskInstanceName, const std::string &storageClassName,
  const uint32_t copyNb) {
  try {
    const char *const sql =
      "DELETE FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot delete archive route for storage-class " << diskInstanceName + ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveRoute> RdbmsCatalogue::getArchiveRoutes() const {
  try {
    std::list<common::dataStructures::ArchiveRoute> routes;
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"
        "ARCHIVE_ROUTE.TAPE_POOL_NAME AS TAPE_POOL_NAME,"

        "ARCHIVE_ROUTE.USER_COMMENT AS USER_COMMENT,"

        "ARCHIVE_ROUTE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "ARCHIVE_ROUTE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "ARCHIVE_ROUTE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "ARCHIVE_ROUTE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "ARCHIVE_ROUTE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "ARCHIVE_ROUTE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "ORDER BY "
        "DISK_INSTANCE_NAME, STORAGE_CLASS_NAME, COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::ArchiveRoute route;

      route.diskInstanceName = rset.columnString("DISK_INSTANCE_NAME");
      route.storageClassName = rset.columnString("STORAGE_CLASS_NAME");
      route.copyNb = rset.columnUint64("COPY_NB");
      route.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      route.comment = rset.columnString("USER_COMMENT");
      route.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      route.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      route.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      route.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      route.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      route.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      routes.push_back(route);
    }

    return routes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &storageClassName, const uint32_t copyNb,
  const std::string &tapePoolName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ARCHIVE_ROUTE SET "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot modify archive route for storage-class " << instanceName + ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &storageClassName, const uint32_t copyNb,
  const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ARCHIVE_ROUTE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot modify archive route for storage-class " << instanceName + ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const bool isDisabled,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(logicalLibraryExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create logical library ") + name +
        " because a logical library with the same name already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO LOGICAL_LIBRARY("
        "LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":LOGICAL_LIBRARY_NAME,"
        ":IS_DISABLED,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.bindBool(":IS_DISABLED", isDisabled);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// logicalLibraryExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::logicalLibraryExists(rdbms::Conn &conn, const std::string &logicalLibraryName) const {
  try {
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME "
      "FROM "
        "LOGICAL_LIBRARY "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteLogicalLibrary(const std::string &name) {
  try {
    const char *const sql =
      "DELETE "
      "FROM LOGICAL_LIBRARY "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME_1 AND "
        "NOT EXISTS (SELECT LOGICAL_LIBRARY_NAME FROM TAPE WHERE LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME_2)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME_1", name);
    stmt.bindString(":LOGICAL_LIBRARY_NAME_2", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the logical library does not exist or if it still contains tapes
    if(0 == stmt.getNbAffectedRows()) {
      if(logicalLibraryExists(conn, name)) {
        throw UserSpecifiedANonEmptyLogicalLibrary(std::string("Cannot delete logical library ") + name +
          " because it contains one or more tapes");
      } else {
        throw UserSpecifiedANonExistentLogicalLibrary(std::string("Cannot delete logical library ") + name +
          " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<common::dataStructures::LogicalLibrary> RdbmsCatalogue::getLogicalLibraries() const {
  try {
    std::list<common::dataStructures::LogicalLibrary> libs;
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED AS IS_DISABLED,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "LOGICAL_LIBRARY "
      "ORDER BY "
        "LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::LogicalLibrary lib;

      lib.name = rset.columnString("LOGICAL_LIBRARY_NAME");
      lib.isDisabled = rset.columnBool("IS_DISABLED");
      lib.comment = rset.columnString("USER_COMMENT");
      lib.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      lib.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      lib.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      lib.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      lib.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      lib.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      libs.push_back(lib);
    }

    return libs;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setLogicalLibraryDisabled
//------------------------------------------------------------------------------
void RdbmsCatalogue::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool disabledValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "IS_DISABLED = :IS_DISABLED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_DISABLED", disabledValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTape(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid,
  const std::string &mediaType,
  const std::string &vendor,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const bool disabled,
  const bool full,
  const bool readOnly,      
  const std::string &comment) {
  // CTA hard code this field to FALSE
  const bool isFromCastor = false;
  try {
    if(vid.empty()) {
      throw UserSpecifiedAnEmptyStringVid("Cannot create tape because the VID is an empty string");
    }

    if(mediaType.empty()) {
      throw UserSpecifiedAnEmptyStringMediaType("Cannot create tape because the media type is an empty string");
    }

    if(vendor.empty()) {
      throw UserSpecifiedAnEmptyStringVendor("Cannot create tape because the vendor is an empty string");
    }

    if(logicalLibraryName.empty()) {
      throw UserSpecifiedAnEmptyStringLogicalLibraryName("Cannot create tape because the logical library name is an"
        " empty string");
    }

    if(tapePoolName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape because the tape pool name is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create tape because the comment is an empty string");
    }

    if(0 == capacityInBytes) {
      throw UserSpecifiedAZeroCapacity("Cannot create tape because the capacity is zero");
    }

    auto conn = m_connPool.getConn();
    if(tapeExists(conn, vid)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid +
        " because a tape with the same volume identifier already exists");
    }
    if(!logicalLibraryExists(conn, logicalLibraryName)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because logical library " +
        logicalLibraryName + " does not exist");
    }
    if(!tapePoolExists(conn, tapePoolName)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because tape pool " +
        tapePoolName + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE("
        "VID,"
        "MEDIA_TYPE,"
        "VENDOR,"
        "LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME,"
        "CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES,"
        "LAST_FSEQ,"
        "IS_DISABLED,"
        "IS_FULL,"
        "IS_READ_ONLY,"
        "IS_FROM_CASTOR,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":VID,"
        ":MEDIA_TYPE,"
        ":VENDOR,"
        ":LOGICAL_LIBRARY_NAME,"
        ":TAPE_POOL_NAME,"
        ":CAPACITY_IN_BYTES,"
        ":DATA_IN_BYTES,"
        ":LAST_FSEQ,"
        ":IS_DISABLED,"
        ":IS_FULL,"
        ":IS_READ_ONLY,"
        ":IS_FROM_CASTOR,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VID", vid);
    stmt.bindString(":MEDIA_TYPE", mediaType);
    stmt.bindString(":VENDOR", vendor);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt.bindUint64(":DATA_IN_BYTES", 0);
    stmt.bindUint64(":LAST_FSEQ", 0);
    stmt.bindBool(":IS_DISABLED", disabled);
    stmt.bindBool(":IS_FULL", full);
    stmt.bindBool(":IS_READ_ONLY", readOnly);
    stmt.bindBool(":IS_FROM_CASTOR", isFromCastor);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaType)
       .add("vendor", vendor)
       .add("logicalLibraryName", logicalLibraryName)
       .add("tapePoolName", tapePoolName)
       .add("capacityInBytes", capacityInBytes)
       .add("isDisabled", disabled ? 1 : 0)
       .add("isFull", full ? 1 : 0)
       .add("isReadOnly", readOnly ? 1 : 0)
       .add("isFromCastor", isFromCastor ? 1 : 0)
       .add("userComment", comment)
       .add("creationLogUserName", admin.username)
       .add("creationLogHostName", admin.host)
       .add("creationLogTime", now);
    lc.log(log::INFO, "Catalogue - user created tape");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapeExists(const std::string &vid) const {
  try {
    auto conn = m_connPool.getConn();
    return tapeExists(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapeExists(rdbms::Conn &conn, const std::string &vid) const {
  try {
    const char *const sql =
      "SELECT "
        "VID AS VID "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskSystemExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskSystemExists(const std::string &name) const {
  try {
    auto conn = m_connPool.getConn();
    return diskSystemExists(conn, name);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskSystemExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskSystemExists(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME "
      "FROM "
        "DISK_SYSTEM "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTape(const std::string &vid) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :DELETE_VID AND "
        "NOT EXISTS (SELECT VID FROM TAPE_FILE WHERE VID = :SELECT_VID)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_VID", vid);
    stmt.bindString(":SELECT_VID", vid);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(tapeExists(conn, vid)) {
        throw UserSpecifiedANonEmptyTape(std::string("Cannot delete tape ") + vid + " because it contains one or more files");
      } else {
        throw UserSpecifiedANonExistentTape(std::string("Cannot delete tape ") + vid + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(const TapeSearchCriteria &searchCriteria) const {
  try {
    auto conn = m_connPool.getConn();
    return getTapes(conn, searchCriteria);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(rdbms::Conn &conn,
  const TapeSearchCriteria &searchCriteria) const {
  if(isSetAndEmpty(searchCriteria.vid)) throw exception::UserError("VID cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.mediaType)) throw exception::UserError("Media type cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.vendor)) throw exception::UserError("Vendor cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.logicalLibrary)) throw exception::UserError("Logical library cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.tapePool)) throw exception::UserError("Tape pool cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.vo)) throw exception::UserError("Virtual organisation cannot be an empty string");

  try {
    std::list<common::dataStructures::Tape> tapes;
    std::string sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "TAPE.MEDIA_TYPE AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "TAPE.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "TAPE_POOL.VO AS VO,"
        "TAPE.ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "TAPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.IS_DISABLED AS IS_DISABLED,"
        "TAPE.IS_FULL AS IS_FULL,"
        "TAPE.IS_READ_ONLY AS IS_READ_ONLY,"
        "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"

        "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
        "TAPE.LABEL_TIME AS LABEL_TIME,"

        "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

        "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"
            
        "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
        "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

        "TAPE.USER_COMMENT AS USER_COMMENT,"

        "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_NAME = TAPE_POOL.TAPE_POOL_NAME";

    if(searchCriteria.vid ||
       searchCriteria.mediaType ||
       searchCriteria.vendor ||
       searchCriteria.logicalLibrary ||
       searchCriteria.tapePool ||
       searchCriteria.vo ||
       searchCriteria.capacityInBytes ||
       searchCriteria.disabled ||
       searchCriteria.full ||
       searchCriteria.readOnly) {
      sql += " WHERE ";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.vid) {
      sql += " TAPE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.mediaType) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.MEDIA_TYPE = :MEDIA_TYPE";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vendor) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.VENDOR = :VENDOR";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.logicalLibrary) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.TAPE_POOL_NAME = :TAPE_POOL_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vo) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE_POOL.VO = :VO";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.capacityInBytes) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.disabled) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_DISABLED = :IS_DISABLED";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.full) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_FULL = :IS_FULL";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.readOnly) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_READ_ONLY = :IS_READ_ONLY";
      addedAWhereConstraint = true;
    }

    sql += " ORDER BY TAPE.VID";

    auto stmt = conn.createStmt(sql);

    if(searchCriteria.vid) stmt.bindString(":VID", searchCriteria.vid.value());
    if(searchCriteria.mediaType) stmt.bindString(":MEDIA_TYPE", searchCriteria.mediaType.value());
    if(searchCriteria.vendor) stmt.bindString(":VENDOR", searchCriteria.vendor.value());
    if(searchCriteria.logicalLibrary) stmt.bindString(":LOGICAL_LIBRARY_NAME", searchCriteria.logicalLibrary.value());
    if(searchCriteria.tapePool) stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    if(searchCriteria.vo) stmt.bindString(":VO", searchCriteria.vo.value());
    if(searchCriteria.capacityInBytes) stmt.bindUint64(":CAPACITY_IN_BYTES", searchCriteria.capacityInBytes.value());
    if(searchCriteria.disabled) stmt.bindBool(":IS_DISABLED", searchCriteria.disabled.value());
    if(searchCriteria.full) stmt.bindBool(":IS_FULL", searchCriteria.full.value());
    if(searchCriteria.readOnly) stmt.bindBool(":IS_READ_ONLY", searchCriteria.readOnly.value());

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.readOnly = rset.columnBool("IS_READ_ONLY");
      tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");
      
      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");
      
      tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
      tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");

      tape.comment = rset.columnString("USER_COMMENT");
      tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      tapes.push_back(tape);
    }

    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapesByVid
//------------------------------------------------------------------------------
common::dataStructures::VidToTapeMap RdbmsCatalogue::getTapesByVid(const std::set<std::string> &vids) const {
  try {
    common::dataStructures::VidToTapeMap vidToTapeMap;
    std::string sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "TAPE.MEDIA_TYPE AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "TAPE.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "TAPE_POOL.VO AS VO,"
        "TAPE.ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "TAPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.IS_DISABLED AS IS_DISABLED,"
        "TAPE.IS_FULL AS IS_FULL,"
        "TAPE.IS_READ_ONLY AS IS_READ_ONLY,"
        "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"    

        "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
        "TAPE.LABEL_TIME AS LABEL_TIME,"

        "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

        "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"
            
        "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
        "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

        "TAPE.USER_COMMENT AS USER_COMMENT,"

        "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_NAME = TAPE_POOL.TAPE_POOL_NAME";

    if(!vids.empty()) {
      sql += " WHERE ";
    }

    {
      for(uint64_t vidNb = 1; vidNb <= vids.size(); vidNb++) {
        if(1 < vidNb) {
          sql += " OR ";
        }
        sql += "TAPE.VID = :VID" + std::to_string(vidNb);
      }
    }

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    // The following should be replaced by an insert into a temporrary table
    // because this code can create many different prepared statements when
    // there should only be one
    {
      uint64_t vidNb = 1;
      for(auto &vid : vids) {
        stmt.bindString(":VID" + std::to_string(vidNb), vid);
        vidNb++;
      }
    }

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.readOnly = rset.columnBool("IS_READ_ONLY");
      tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");
      
      tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
      tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");

      tape.comment = rset.columnString("USER_COMMENT");
      tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      vidToTapeMap[tape.vid] = tape;
    }

    if(vids.size() != vidToTapeMap.size()) {
      throw exception::Exception("Not all tapes were found");
    }

    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAllTapes
//------------------------------------------------------------------------------
common::dataStructures::VidToTapeMap RdbmsCatalogue::getAllTapes() const {
  try {
    common::dataStructures::VidToTapeMap vidToTapeMap;
    std::string sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "TAPE.MEDIA_TYPE AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "TAPE.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "TAPE_POOL.VO AS VO,"
        "TAPE.ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "TAPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.IS_DISABLED AS IS_DISABLED,"
        "TAPE.IS_FULL AS IS_FULL,"
        "TAPE.IS_READ_ONLY AS IS_READ_ONLY,"
        "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"    

        "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
        "TAPE.LABEL_TIME AS LABEL_TIME,"

        "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

        "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"
                            
        "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
        "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

        "TAPE.USER_COMMENT AS USER_COMMENT,"

        "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_NAME = TAPE_POOL.TAPE_POOL_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.readOnly = rset.columnBool("IS_READ_ONLY");
      tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");
      
      tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
      tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");

      tape.comment = rset.columnString("USER_COMMENT");
      tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      vidToTapeMap[tape.vid] = tape;
    }

    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//getNbNonSupersededFilesOnTape
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbNonSupersededFilesOnTape(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char *const sql = 
    "SELECT COUNT(*) AS NB_NON_SUPERSEDED_FILES FROM TAPE_FILE "
    "WHERE VID = :VID "
    "AND SUPERSEDED_BY_VID IS NULL "
    "AND SUPERSEDED_BY_FSEQ IS NULL";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    rset.next();
    return rset.columnUint64("NB_NON_SUPERSEDED_FILES");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// getNbFilesOnTape
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbFilesOnTape(const std::string& vid) const {
  try {
    auto conn = m_connPool.getConn();
    return getNbFilesOnTape(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//getNbFilesOnTape
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbFilesOnTape(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char *const sql = 
    "SELECT COUNT(*) AS NB_FILES FROM TAPE_FILE "
    "WHERE VID = :VID ";
    
    auto stmt = conn.createStmt(sql);
    
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    rset.next();
    return rset.columnUint64("NB_FILES");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::deleteTapeFiles(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char * const sql = 
    "DELETE FROM TAPE_FILE WHERE VID = :VID "
    "AND SUPERSEDED_BY_VID IS NOT NULL "
    "AND SUPERSEDED_BY_FSEQ IS NOT NULL";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::resetTapeCounters(rdbms::Conn& conn, const common::dataStructures::SecurityIdentity& admin, const std::string& vid) const {
  try {
    const time_t now = time(nullptr);
    const char * const sql =
    "UPDATE TAPE SET "
        "DATA_IN_BYTES = 0,"
        "LAST_FSEQ = 0,"
        "IS_FULL = '0',"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  } catch (exception::Exception &ex){
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid) {
  try{
    auto conn = m_connPool.getConn();
    
    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);

    if(tapes.empty()) {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it does not exist");
    }  else {
      if(!tapes.front().full){
        throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it is not FULL");
      } 
    }
    //The tape exists and is full, we can try to reclaim it
    if(getNbNonSupersededFilesOnTape(conn,vid) == 0){
      //There is no non-superseded files on the tape, we can reclaim it : delete the files and reset the counters
      deleteTapeFiles(conn,vid);
      resetTapeCounters(conn,admin,vid);
    } else {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because there is at least one tape"
            " file in the catalogue that is on the tape");
    }
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkTapeForLabel
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeForLabel(const std::string &vid) {
   try{
    auto conn = m_connPool.getConn();
    
    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);

    if(tapes.empty()) {
      throw exception::UserError(std::string("Cannot label tape ") + vid + 
                                             " because it does not exist");
    } 
    //The tape exists checks any files on it
    const uint64_t nbFilesOnTape = getNbFilesOnTape(conn, vid);
    if( 0 != nbFilesOnTape) {
      throw exception::UserError(std::string("Cannot label tape ") + vid + 
                                             " because it has " +
                                             std::to_string(nbFilesOnTape) + 
                                             " file(s)");  
    } 
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeLogFromRset
//------------------------------------------------------------------------------
optional<common::dataStructures::TapeLog> RdbmsCatalogue::getTapeLogFromRset(const rdbms::Rset &rset,
  const std::string &driveColName, const std::string &timeColName) const {
  try {
    const optional<std::string> drive = rset.columnOptionalString(driveColName);
    const optional<uint64_t> time = rset.columnOptionalUint64(timeColName);

    if(!drive && !time) {
      return nullopt;
    }

    if(drive && !time) {
      throw exception::Exception(std::string("Database column ") + driveColName + " contains " + drive.value() +
        " but column " + timeColName + " is nullptr");
    }

    if(time && !drive) {
      throw exception::Exception(std::string("Database column ") + timeColName + " contains " +
        std::to_string(time.value()) + " but column " + driveColName + " is nullptr");
    }

    common::dataStructures::TapeLog tapeLog;
    tapeLog.drive = drive.value();
    tapeLog.time = time.value();

    return tapeLog;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeMediaType
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &mediaType) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "MEDIA_TYPE = :MEDIA_TYPE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE", mediaType);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaType)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - mediaType");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeVendor
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &vendor) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "VENDOR = :VENDOR,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VENDOR", vendor);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("vendor", vendor)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - vendor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &logicalLibraryName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("logicalLibraryName", logicalLibraryName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - logicalLibraryName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &tapePoolName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("tapePoolName", tapePoolName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - tapePoolName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const uint64_t capacityInBytes) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("capacityInBytes", capacityInBytes)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - capacityInBytes");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &encryptionKey) {
  try {
    optional<std::string> optionalEncryptionKey;
    if(!encryptionKey.empty()) {
      optionalEncryptionKey = encryptionKey;
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "ENCRYPTION_KEY = :ENCRYPTION_KEY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindOptionalString(":ENCRYPTION_KEY", optionalEncryptionKey);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("encryptionKey", optionalEncryptionKey ? optionalEncryptionKey.value() : "NULL")
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - encryptionKey");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForArchive
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeMountedForArchive(const std::string &vid, const std::string &drive) {
  try {  
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME, "
        "WRITE_MOUNT_COUNT = WRITE_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_WRITE_DRIVE", drive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastWriteDrive", drive)
       .add("lastWriteTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForArchive");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForRetrieve
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeMountedForRetrieve(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_READ_DRIVE = :LAST_READ_DRIVE,"
        "LAST_READ_TIME = :LAST_READ_TIME, "
        "READ_MOUNT_COUNT = READ_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_READ_DRIVE", drive);
    stmt.bindUint64(":LAST_READ_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastReadDrive", drive)
       .add("lastReadTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForRetrieve");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool fullValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = :IS_FULL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_FULL", fullValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", fullValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// noSpaceLeftOnTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::noSpaceLeftOnTape(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", 1)
       .add("method", "noSpaceLeftOnTape");
    lc.log(log::INFO, "Catalogue - system modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeReadOnly
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeReadOnly(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool readOnlyValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_READ_ONLY = :IS_READ_ONLY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_READ_ONLY", readOnlyValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isReadOnly", readOnlyValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - isReadOnly");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeReadOnlyOnError
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeReadOnlyOnError(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_READ_ONLY = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isReadOnly", 1)
       .add("method", "setTapeReadOnlyOnError");
    lc.log(log::INFO, "Catalogue - system modified tape - isReadOnly");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeIsFromCastorInUnitTests
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeIsFromCastorInUnitTests(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FROM_CASTOR = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFromCastor", 1)
       .add("method", "setTapeIsFromCastorInUnitTests");
    lc.log(log::INFO, "Catalogue - system modified tape - isFromCastor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const bool disabledValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_DISABLED = :IS_DISABLED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_DISABLED", disabledValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isDisabled", disabledValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - isDisabled");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("userComment", comment)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - userComment");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterMountRulePolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_MOUNT_RULE SET "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester mount rule ") + instanceName + ":" +
        requesterName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesteMountRuleComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_MOUNT_RULE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester mount rule ") + instanceName + ":" +
        requesterName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterGroupMountRulePolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterGroupMountRuleComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::createMountPolicy(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const uint64_t archivePriority,
  const uint64_t minArchiveRequestAge,
  const uint64_t retrievePriority,
  const uint64_t minRetrieveRequestAge,
  const uint64_t maxDrivesAllowed,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(mountPolicyExists(conn, name)) {
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

        "MAX_DRIVES_ALLOWED,"

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

        ":MAX_DRIVES_ALLOWED,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":MOUNT_POLICY_NAME", name);

    stmt.bindUint64(":ARCHIVE_PRIORITY", archivePriority);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);

    stmt.bindUint64(":RETRIEVE_PRIORITY", retrievePriority);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);

    stmt.bindUint64(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// createRequesterMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::createRequesterMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &comment) {
  try {
    const auto user = User(diskInstanceName, requesterName);
    auto conn = m_connPool.getConn();
    const auto mountPolicy = getRequesterMountPolicy(conn, user);
    if(mountPolicy) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName +
        " because the requester is already assigned to mount-policy " + mountPolicy->name);
    }
    if(!mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " because mount-policy " + mountPolicyName +
        " does not exist");
    }
    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO REQUESTER_MOUNT_RULE("
        "DISK_INSTANCE_NAME,"
        "REQUESTER_NAME,"
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
        ":REQUESTER_NAME,"
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
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// getRequesterMountRules
//------------------------------------------------------------------------------
std::list<common::dataStructures::RequesterMountRule> RdbmsCatalogue::getRequesterMountRules() const {
  try {
    std::list<common::dataStructures::RequesterMountRule> rules;
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "REQUESTER_NAME AS REQUESTER_NAME,"
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "ORDER BY "
        "DISK_INSTANCE_NAME, REQUESTER_NAME, MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while(rset.next()) {
      common::dataStructures::RequesterMountRule rule;

      rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      rule.name = rset.columnString("REQUESTER_NAME");
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
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteRequesterMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) {
  try {
    const char *const sql =
      "DELETE FROM "
        "REQUESTER_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":" + requesterName +
        " because the rule does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createRequesterGroupMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterGroupName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    {
      const auto group = Group(diskInstanceName, requesterGroupName);
      const auto mountPolicy = getRequesterGroupMountPolicy(conn, group);
      if (mountPolicy) {
        throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
                                   " to requester-group " + diskInstanceName + ":" + requesterGroupName +
                                   " because a rule already exists assigning the requester-group to mount-policy " +
                                   mountPolicy->name);
      }
    }
    if(!mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot assign mount-policy ") + mountPolicyName + " to requester-group " +
        diskInstanceName + ":" + requesterGroupName + " because mount-policy " + mountPolicyName + " does not exist");
    }
    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO REQUESTER_GROUP_MOUNT_RULE("
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

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// getCachedRequesterGroupMountPolicy
//------------------------------------------------------------------------------
optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getCachedRequesterGroupMountPolicy(const Group &group)
  const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getRequesterGroupMountPolicy(conn, group);
    };
    return m_groupMountPolicyCache.getCachedValue(group, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterGroupMountPolicy
//------------------------------------------------------------------------------
optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getRequesterGroupMountPolicy(
  rdbms::Conn &conn, const Group &group) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

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

      policy.maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");

      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    } else {
      return nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterGroupMountRules
//------------------------------------------------------------------------------
std::list<common::dataStructures::RequesterGroupMountRule> RdbmsCatalogue::getRequesterGroupMountRules() const {
  try {
    std::list<common::dataStructures::RequesterGroupMountRule> rules;
    const char *const sql =
      "SELECT "
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
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while(rset.next()) {
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
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteRequesterGroupMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteRequesterGroupMountRule(const std::string &diskInstanceName,
  const std::string &requesterGroupName) {
  try {
    const char *const sql =
      "DELETE FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete the mount rule for requester group ") + diskInstanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// mountPolicyExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::mountPolicyExists(rdbms::Conn &conn, const std::string &mountPolicyName) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME "
      "FROM "
        "MOUNT_POLICY "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// requesterMountRuleExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::requesterMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterName) const {
  try {
    const char *const sql =
      "SELECT "
        "REQUESTER_NAME AS REQUESTER_NAME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedRequesterMountPolicy
//------------------------------------------------------------------------------
optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getCachedRequesterMountPolicy(const User &user) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getRequesterMountPolicy(conn, user);
    };
    return m_userMountPolicyCache.getCachedValue(user, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterMountPolicy
//------------------------------------------------------------------------------
optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getRequesterMountPolicy(rdbms::Conn &conn,
  const User &user) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

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

      policy.maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");

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
      return nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// requesterGroupMountRuleExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::requesterGroupMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterGroupName) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "REQUESTER_GROUP_NAME AS REQUESTER_GROUP_NAME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteMountPolicy(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM MOUNT_POLICY WHERE MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
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
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
std::list<common::dataStructures::MountPolicy> RdbmsCatalogue::getMountPolicies() const {
  try {
    std::list<common::dataStructures::MountPolicy> policies;
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

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
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");

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

//------------------------------------------------------------------------------
// modifyMountPolicyArchivePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrievePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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
}

//------------------------------------------------------------------------------
// modifyMountPolicyMaxDrivesAllowed
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t maxDrivesAllowed) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "MAX_DRIVES_ALLOWED = :MAX_DRIVES_ALLOWED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);
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
}

//------------------------------------------------------------------------------
// modifyMountPolicyComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
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
}

//------------------------------------------------------------------------------
// createActivitiesFairShareWeight
//------------------------------------------------------------------------------
void RdbmsCatalogue::createActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity& admin, 
    const std::string& diskInstanceName, const std::string& activity, double weight, const std::string & comment) {
  try {
    if (diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create activity weight because the disk instance name is"
        " an empty string");
    }
    
    if (activity.empty()) {
      throw UserSpecifiedAnEmptyStringActivity("Cannot create activity weight because the activity name is"
        " an empty string");
    }
    
    if (weight <= 0 || weight > 1) {
      throw UserSpecifiedAnOutOfRangeActivityWeight("Cannot create activity because the activity weight is out of ]0, 1] range.");
    }
    
    if (comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create activity weight because the comment is"
        " an empty string");
    }
    
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ACTIVITIES_WEIGHTS("
        "DISK_INSTANCE_NAME,"
        "ACTIVITY,"
        "WEIGHT,"
    
        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
    
      "VALUES ("
        ":DISK_INSTANCE_NAME,"
        ":ACTIVITY,"
        ":WEIGHT,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":ACTIVITY", activity);
    stmt.bindString(":WEIGHT", std::to_string(weight));
    
    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    
    stmt.executeNonQuery();

    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyActivitiesFairShareWeight
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity& admin, const std::string& diskInstanceName, const std::string& activity, double weight, const std::string& comment) {
  try {
    if (diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create activity weight because the disk instance name is"
        " an empty string");
    }
    
    if (activity.empty()) {
      throw UserSpecifiedAnEmptyStringActivity("Cannot create activity weight because the activity name is"
        " an empty string");
    }
    
    if (weight <= 0 || weight > 1) {
      throw UserSpecifiedAnOutOfRangeActivityWeight("Cannot create activity because the activity weight is out of ]0, 1] range.");
    }
    
    if (comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify activity weight because the comment is"
        " an empty string");
    }
    
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ACTIVITIES_WEIGHTS SET "
        "WEIGHT = :WEIGHT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME,"
        "USER_COMMENT = :USER_COMMENT "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ACTIVITY = :ACTIVITY";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":ACTIVITY", activity);
    stmt.bindString(":WEIGHT", std::to_string(weight));
    
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify activity fair share weight ") + activity + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// deleteActivitiesFairShareWeight
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity& admin, const std::string& diskInstanceName, const std::string& activity) {
  try {
    if (diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create activity weight because the disk instance name is"
        " an empty string");
    }
    
    if (activity.empty()) {
      throw UserSpecifiedAnEmptyStringActivity("Cannot create activity weight because the activity name is"
        " an empty string");
    }
    
    const char *const sql = "DELETE FROM ACTIVITIES_WEIGHTS WHERE DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND ACTIVITY = :ACTIVITY";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":ACTIVITY", activity);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete activity weight ") + activity + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getActivitiesFairShareWeights
//------------------------------------------------------------------------------
std::list<common::dataStructures::ActivitiesFairShareWeights> RdbmsCatalogue::getActivitiesFairShareWeights() const {
  try {
    std::string sql =
      "SELECT "
        "ACTIVITIES_WEIGHTS.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ACTIVITIES_WEIGHTS.ACTIVITY AS ACTIVITY,"
        "ACTIVITIES_WEIGHTS.WEIGHT AS WEIGHT "
      "FROM "
        "ACTIVITIES_WEIGHTS";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    std::map<std::string, common::dataStructures::ActivitiesFairShareWeights> activitiesMap;
    while(rset.next()) {
      common::dataStructures::ActivitiesFairShareWeights * activity;
      auto diskInstanceName = rset.columnString("DISK_INSTANCE_NAME");
      try {
        activity = & activitiesMap.at(diskInstanceName);
      } catch (std::out_of_range) {
        activity = & activitiesMap[diskInstanceName];
        activity->diskInstance = diskInstanceName;
      }
      activity->setWeightFromString(rset.columnString("ACTIVITY"), rset.columnString("WEIGHT"));
    }
    std::list<common::dataStructures::ActivitiesFairShareWeights> ret;
    for (auto & dia: activitiesMap) {
      ret.push_back(dia.second);
    }
    return ret;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAllDiskSystems
//------------------------------------------------------------------------------
disk::DiskSystemList RdbmsCatalogue::getAllDiskSystems() const {
  try {
    disk::DiskSystemList diskSystemList;
    std::string sql =
      "SELECT "
        "DISK_SYSTEM.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "DISK_SYSTEM.FILE_REGEXP AS FILE_REGEXP,"
        "DISK_SYSTEM.FREE_SPACE_QUERY_URL AS FREE_SPACE_QUERY_URL,"
        "DISK_SYSTEM.REFRESH_INTERVAL AS REFRESH_INTERVAL,"
        "DISK_SYSTEM.TARGETED_FREE_SPACE AS TARGETED_FREE_SPACE,"
        "DISK_SYSTEM.SLEEP_TIME AS SLEEP_TIME,"
        
        "DISK_SYSTEM.USER_COMMENT AS USER_COMMENT,"

        "DISK_SYSTEM.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DISK_SYSTEM.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DISK_SYSTEM.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_SYSTEM.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "DISK_SYSTEM";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      disk::DiskSystem diskSystem;
      diskSystem.name = rset.columnString("DISK_SYSTEM_NAME");
      diskSystem.fileRegexp = rset.columnString("FILE_REGEXP");
      diskSystem.freeSpaceQueryURL = rset.columnString("FREE_SPACE_QUERY_URL");
      diskSystem.refreshInterval =  rset.columnUint64("REFRESH_INTERVAL");
      diskSystem.targetedFreeSpace =  rset.columnUint64("TARGETED_FREE_SPACE");
      diskSystem.sleepTime =  rset.columnUint64("SLEEP_TIME");
      diskSystem.comment = rset.columnString("USER_COMMENT");
      diskSystem.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      diskSystem.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      diskSystem.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      diskSystem.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      diskSystem.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      diskSystem.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
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

//------------------------------------------------------------------------------
// createDiskSystem
//------------------------------------------------------------------------------
void RdbmsCatalogue::createDiskSystem(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &fileRegexp,
  const std::string &freeSpaceQueryURL,
  const uint64_t refreshInterval,
  const uint64_t targetedFreeSpace,
  const uint64_t sleepTime,
  const std::string &comment) {
 try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot create disk system because the name is an empty string");
    }
    if(fileRegexp.empty()) {
      throw UserSpecifiedAnEmptyStringFileRegexp("Cannot create disk system because the file regexp is an empty string");
    }
    if(freeSpaceQueryURL.empty()) {
      throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot create disk system because the free space query URL is an empty string");
    }
    if(0 == refreshInterval) {
      throw UserSpecifiedAZeroRefreshInterval("Cannot create disk system because the refresh interval is zero");
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

    auto conn = m_connPool.getConn();
    if(diskSystemExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create disk system ") + name +
        " because a disk system with the same name identifier already exists");
    }
    
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO DISK_SYSTEM("
        "DISK_SYSTEM_NAME,"
        "FILE_REGEXP,"
        "FREE_SPACE_QUERY_URL,"
        "REFRESH_INTERVAL,"
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
        ":FILE_REGEXP,"
        ":FREE_SPACE_QUERY_URL,"
        ":REFRESH_INTERVAL,"
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
    stmt.bindString(":FILE_REGEXP", fileRegexp);
    stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
    stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
    stmt.bindUint64(":TARGETED_FREE_SPACE", targetedFreeSpace);
    stmt.bindUint64(":SLEEP_TIME", sleepTime);

    stmt.bindString(":USER_COMMENT", comment);

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

//------------------------------------------------------------------------------
// deleteDiskSystem
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteDiskSystem(const std::string &name) {
    try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DISK_SYSTEM "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(diskSystemExists(conn, name)) {
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

void RdbmsCatalogue::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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

void RdbmsCatalogue::modifyDiskSystemFreeSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &freeSpaceQueryURL) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(freeSpaceQueryURL.empty()) {
      throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot modify disk system "
        "because the new freeSpaceQueryURL is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "FREE_SPACE_QUERY_URL = :FREE_SPACE_QUERY_URL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
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

void RdbmsCatalogue::modifyDiskSystemRefreshInterval(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t refreshInterval) {
    try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(0 == refreshInterval) {
      throw UserSpecifiedAZeroRefreshInterval("Cannot modify disk system "
        "because the new refresh interval has zero value");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "REFRESH_INTERVAL = :REFRESH_INTERVAL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
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

void RdbmsCatalogue::modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool.getConn();
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

void RdbmsCatalogue::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
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

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
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

//------------------------------------------------------------------------------
// modifyDiskSystemSleepTime
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, 
    const uint64_t sleepTime) {
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
    auto conn = m_connPool.getConn();
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

//------------------------------------------------------------------------------
// insertArchiveFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertArchiveFile(rdbms::Conn &conn, const ArchiveFileRow &row) {
  try {
    if(!storageClassExists(conn, row.diskInstance, row.storageClassName)) {
      throw exception::UserError(std::string("Storage class ") + row.diskInstance + ":" + row.storageClassName +
        " does not exist");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ARCHIVE_FILE("
        "ARCHIVE_FILE_ID,"
        "DISK_INSTANCE_NAME,"
        "DISK_FILE_ID,"
        "DISK_FILE_PATH,"
        "DISK_FILE_UID,"
        "DISK_FILE_GID,"
        "SIZE_IN_BYTES,"
        "CHECKSUM_BLOB,"
        "CHECKSUM_ADLER32,"
        "STORAGE_CLASS_ID,"
        "CREATION_TIME,"
        "RECONCILIATION_TIME)"
      "SELECT "
        ":ARCHIVE_FILE_ID,"
        "DISK_INSTANCE_NAME,"
        ":DISK_FILE_ID,"
        ":DISK_FILE_PATH,"
        ":DISK_FILE_UID,"
        ":DISK_FILE_GID,"
        ":SIZE_IN_BYTES,"
        ":CHECKSUM_BLOB,"
        ":CHECKSUM_ADLER32,"
        "STORAGE_CLASS_ID,"
        ":CREATION_TIME,"
        ":RECONCILIATION_TIME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":ARCHIVE_FILE_ID", row.archiveFileId);
    stmt.bindString(":DISK_INSTANCE_NAME", row.diskInstance);
    stmt.bindString(":DISK_FILE_ID", row.diskFileId);
    stmt.bindString(":DISK_FILE_PATH", row.diskFilePath);
    stmt.bindUint64(":DISK_FILE_UID", row.diskFileOwnerUid);
    stmt.bindUint64(":DISK_FILE_GID", row.diskFileGid);
    stmt.bindUint64(":SIZE_IN_BYTES", row.size);
    stmt.bindBlob  (":CHECKSUM_BLOB", row.checksumBlob.serialize());
    // Keep transition ADLER32 checksum up-to-date if it exists
    uint32_t adler32;
    try {
      std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(row.checksumBlob.at(checksum::ADLER32));
      adler32 = strtoul(adler32hex.c_str(), 0, 16);
    } catch(exception::ChecksumTypeMismatch &ex) {
      adler32 = 0;
    }
    stmt.bindUint64(":CHECKSUM_ADLER32", adler32);
    stmt.bindString(":STORAGE_CLASS_NAME", row.storageClassName);
    stmt.bindUint64(":CREATION_TIME", now);
    stmt.bindUint64(":RECONCILIATION_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: archiveFileId=" + std::to_string(row.archiveFileId) +
       ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkTapeFileSearchCriteria
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileSearchCriteria(const TapeFileSearchCriteria &searchCriteria) const {
  auto conn = m_connPool.getConn();

  if(searchCriteria.archiveFileId) {
    if(!archiveFileIdExists(conn, searchCriteria.archiveFileId.value())) {
      throw exception::UserError(std::string("Archive file with ID ") +
        std::to_string(searchCriteria.archiveFileId.value()) + " does not exist");
    }
  }

  if(searchCriteria.diskFileGid && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file group ") + std::to_string(searchCriteria.diskFileGid.value()) +
      " is ambiguous without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFileGid) {
    if(!diskFileGroupExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFileGid.value())) {
      throw exception::UserError(std::string("Disk file group ") + searchCriteria.diskInstance.value() + "::" +
        std::to_string(searchCriteria.diskFileGid.value()) + " does not exist");
    }
  }

  if(searchCriteria.diskFileId && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file ID ") + searchCriteria.diskFileId.value() + " is ambiguous "
      "without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFileId) {
    if(!diskFileIdExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFileId.value())) {
      throw exception::UserError(std::string("Disk file ID ") + searchCriteria.diskInstance.value() + "::" +
        searchCriteria.diskFileId.value() + " does not exist");
    }
  }

  if(searchCriteria.diskFilePath && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file path ") + searchCriteria.diskFilePath.value() + " is ambiguous "
      "without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFilePath) {
    if(!diskFilePathExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFilePath.value())) {
      throw exception::UserError(std::string("Disk file path ") + searchCriteria.diskInstance.value() + "::" +
        searchCriteria.diskFilePath.value() + " does not exist");
    }
  }

  if(searchCriteria.diskFileOwnerUid && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file user ") + std::to_string(searchCriteria.diskFileOwnerUid.value()) +
      " is ambiguous without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFileOwnerUid) {
    if(!diskFileUserExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFileOwnerUid.value())) {
      throw exception::UserError(std::string("Disk file user ") + searchCriteria.diskInstance.value() + "::" +
        std::to_string(searchCriteria.diskFileOwnerUid.value()) + " does not exist");
    }
  }

  if(searchCriteria.storageClass && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Storage class ") + searchCriteria.storageClass.value() + " is ambiguous "
      "without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.storageClass) {
    if(!storageClassExists(conn, searchCriteria.diskInstance.value(), searchCriteria.storageClass.value())) {
      throw exception::UserError(std::string("Storage class ") + searchCriteria.diskInstance.value() + "::" +
        searchCriteria.storageClass.value() + " does not exist");
    }
  }

  if(searchCriteria.tapePool) {
    if(!tapePoolExists(conn, searchCriteria.tapePool.value())) {
      throw exception::UserError(std::string("Tape pool ") + searchCriteria.tapePool.value() + " does not exist");
    }
  }

  if(searchCriteria.vid) {
    if(!tapeExists(conn, searchCriteria.vid.value())) {
      throw exception::UserError(std::string("Tape ") + searchCriteria.vid.value() + " does not exist");
    }
  }
}

//------------------------------------------------------------------------------
// getArchiveFilesItor
//------------------------------------------------------------------------------
ArchiveFileItor RdbmsCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const {

  checkTapeFileSearchCriteria(searchCriteria);

  try {
    auto impl = new RdbmsCatalogueGetArchiveFilesItor(m_log, m_archiveFileListingConnPool, searchCriteria);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getFilesForRepack
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveFile> RdbmsCatalogue::getFilesForRepack(
  const std::string &vid,
  const uint64_t startFSeq,
  const uint64_t maxNbFiles) const {
  try {
    std::string sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ,"
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
       "WHERE "
         "TAPE_FILE.VID = :VID AND "
         "TAPE_FILE.FSEQ >= :START_FSEQ AND "
         "TAPE_FILE.SUPERSEDED_BY_VID IS NULL "
       "ORDER BY FSEQ";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":START_FSEQ", startFSeq);
    auto rset = stmt.executeQuery();

    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    while(rset.next()) {
      common::dataStructures::ArchiveFile archiveFile;

      archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
      archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
      archiveFile.diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
      archiveFile.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
      archiveFile.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
      archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
      archiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
      archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
      archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
      archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = rset.columnUint64("COPY_NB");
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = archiveFile.checksumBlob; // Duplicated for convenience
      if (!rset.columnIsNull("SSBY_VID")) {
        tapeFile.supersededByVid = rset.columnString("SSBY_VID");
        tapeFile.supersededByFSeq = rset.columnUint64("SSBY_VID");
      }

      archiveFile.tapeFiles.push_back(tapeFile);

      archiveFiles.push_back(archiveFile);

      if(maxNbFiles == archiveFiles.size()) break;
    }
    return archiveFiles;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileItorForRepack
//------------------------------------------------------------------------------
ArchiveFileItor RdbmsCatalogue::getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const {
  try {
    auto impl = new RdbmsCatalogueGetArchiveFilesForRepackItor(m_log, m_archiveFileListingConnPool, vid, startFSeq);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeFileSummary
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileSummary RdbmsCatalogue::getTapeFileSummary(
  const TapeFileSearchCriteria &searchCriteria) const {
  try {
    std::string sql =
      "SELECT "
        "COALESCE(SUM(ARCHIVE_FILE.SIZE_IN_BYTES), 0) AS TOTAL_BYTES,"
        "COUNT(ARCHIVE_FILE.ARCHIVE_FILE_ID) AS TOTAL_FILES "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID";

    if(
      searchCriteria.archiveFileId  ||
      searchCriteria.diskInstance   ||
      searchCriteria.diskFileId     ||
      searchCriteria.diskFilePath   ||
      searchCriteria.diskFileOwnerUid   ||
      searchCriteria.diskFileGid  ||
      searchCriteria.storageClass   ||
      searchCriteria.vid            ||
      searchCriteria.tapeFileCopyNb ||
      searchCriteria.tapePool) {
      sql += " WHERE ";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.archiveFileId) {
      sql += " ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskInstance) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileId) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFilePath) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_PATH = :DISK_FILE_PATH";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileOwnerUid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_UID = :DISK_FILE_UID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileGid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_GID = :DISK_FILE_GID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.storageClass) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapeFileCopyNb) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.COPY_NB = :TAPE_FILE_COPY_NB";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    }

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    if(searchCriteria.archiveFileId) {
      stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }
    if(searchCriteria.diskInstance) {
      stmt.bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
    }
    if(searchCriteria.diskFileId) {
      stmt.bindString(":DISK_FILE_ID", searchCriteria.diskFileId.value());
    }
    if(searchCriteria.diskFilePath) {
      stmt.bindString(":DISK_FILE_PATH", searchCriteria.diskFilePath.value());
    }
    if(searchCriteria.diskFileOwnerUid) {
      stmt.bindUint64(":DISK_FILE_UID", searchCriteria.diskFileOwnerUid.value());
    }
    if(searchCriteria.diskFileGid) {
      stmt.bindUint64(":DISK_FILE_GID", searchCriteria.diskFileGid.value());
    }
    if(searchCriteria.storageClass) {
      stmt.bindString(":STORAGE_CLASS_NAME", searchCriteria.storageClass.value());
    }
    if(searchCriteria.vid) {
      stmt.bindString(":VID", searchCriteria.vid.value());
    }
    if(searchCriteria.tapeFileCopyNb) {
      stmt.bindUint64(":TAPE_FILE_COPY_NB", searchCriteria.tapeFileCopyNb.value());
    }
    if(searchCriteria.tapePool) {
      stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    }
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("SELECT COUNT statement did not returned a row");
    }

    common::dataStructures::ArchiveFileSummary summary;
    summary.totalBytes = rset.columnUint64("TOTAL_BYTES");
    summary.totalFiles = rset.columnUint64("TOTAL_FILES");
    return summary;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogue::getArchiveFileById(const uint64_t id) {
  try {
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile(getArchiveFileByArchiveFileId(conn, id));

    // Throw an exception if the archive file does not exist
    if(nullptr == archiveFile.get()) {
      exception::Exception ex;
      ex.getMessage() << "No such archive file with ID " << id;
      throw (ex);
    }

    return *archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeLabelled
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeLabelled(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LABEL_DRIVE = :LABEL_DRIVE,"
        "LABEL_TIME = :LABEL_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LABEL_DRIVE", drive);
    stmt.bindUint64(":LABEL_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkAndGetNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  try {
    const auto storageClass = StorageClass(diskInstanceName, storageClassName);
    const auto copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
    const auto expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " has no archive routes";
      throw ue;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ue;
    }

    auto mountPolicy = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));
    // Only consider the requester's group if there is no user mount policy
    if(!mountPolicy) {
      const auto groupMountPolicy = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));

      if(!groupMountPolicy) {
        exception::UserError ue;
        ue.getMessage() << "No mount rules for the requester or their group:"
          " storageClass=" << storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" <<
          user.group;
        throw ue;
      }
    }

    // Now that we have found both the archive routes and the mount policy it's
    // safe to consume an archive file identifier
    {
      auto conn = m_connPool.getConn();
      return getNextArchiveFileId(conn);
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileQueueCriteria
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileQueueCriteria RdbmsCatalogue::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  try {
    const StorageClass storageClass = StorageClass(diskInstanceName, storageClassName);
    const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
    const uint64_t expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " has no archive routes";
      throw ue;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ue;
    }

    // Get the mount policy - user mount policies overrule group ones
    auto mountPolicy = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));
    if(!mountPolicy) {
      mountPolicy = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));

      if(!mountPolicy) {
        exception::UserError ue;
        ue.getMessage() << "No mount rules for the requester or their group: storageClass=" << storageClassName <<
          " requester=" << diskInstanceName << ":" << user.name << ":" << user.group;
        throw ue;
      }
    }

    return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *mountPolicy);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedTapeCopyToPoolMap
//------------------------------------------------------------------------------
common::dataStructures::TapeCopyToPoolMap RdbmsCatalogue::getCachedTapeCopyToPoolMap(const StorageClass &storageClass)
  const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getTapeCopyToPoolMap(conn, storageClass);
    };
    return m_tapeCopyToPoolCache.getCachedValue(storageClass, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeCopyToPoolMap
//------------------------------------------------------------------------------
common::dataStructures::TapeCopyToPoolMap RdbmsCatalogue::getTapeCopyToPoolMap(rdbms::Conn &conn,
  const StorageClass &storageClass) const {
  try {
    common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
    const char *const sql =
      "SELECT "
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"
        "ARCHIVE_ROUTE.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", storageClass.diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      const uint32_t copyNb = rset.columnUint64("COPY_NB");
      const std::string tapePoolName = rset.columnString("TAPE_POOL_NAME");
      copyToPoolMap[copyNb] = tapePoolName;
    }

    return copyToPoolMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getCachedExpectedNbArchiveRoutes(const StorageClass &storageClass) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getExpectedNbArchiveRoutes(conn, storageClass);
    };
    return m_expectedNbArchiveRoutesCache.getCachedValue(storageClass, getNonCachedValue);
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}


//------------------------------------------------------------------------------
// getExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getExpectedNbArchiveRoutes(rdbms::Conn &conn, const StorageClass &storageClass) const {
  try {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROUTES "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", storageClass.diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset.columnUint64("NB_ROUTES");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::updateTape(
  rdbms::Conn &conn,
  const std::string &vid,
  const uint64_t lastFSeq,
  const uint64_t compressedBytesWritten,
  const std::string &tapeDrive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ,"
        "DATA_IN_BYTES = DATA_IN_BYTES + :DATA_IN_BYTES,"
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.bindUint64(":DATA_IN_BYTES", compressedBytesWritten);
    stmt.bindString(":LAST_WRITE_DRIVE", tapeDrive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// prepareToRetrieveFile
//------------------------------------------------------------------------------
common::dataStructures::RetrieveFileQueueCriteria RdbmsCatalogue::prepareToRetrieveFile(
  const std::string &diskInstanceName,
  const uint64_t archiveFileId,
  const common::dataStructures::RequesterIdentity &user,
  const optional<std::string>& activity,
  log::LogContext &lc) {
  try {
    cta::utils::Timer t;
    common::dataStructures::RetrieveFileQueueCriteria criteria;
    {
      auto conn = m_connPool.getConn();
      const auto getConnTime = t.secs(utils::Timer::resetCounter);
      auto archiveFile = getArchiveFileToRetrieveByArchiveFileId(conn, archiveFileId);
      const auto getArchiveFileTime = t.secs(utils::Timer::resetCounter);
      if(nullptr == archiveFile.get()) {
        exception::UserError ex;
        ex.getMessage() << "No tape files available for archive file with archive file ID " << archiveFileId;
        throw ex;
      }

      if(diskInstanceName != archiveFile->diskInstance) {
        exception::UserError ue;
        ue.getMessage() << "Cannot retrieve file because the disk instance of the request does not match that of the"
          " archived file: archiveFileId=" << archiveFileId << " path=" << archiveFile->diskFileInfo.path <<
          " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" << archiveFile->diskInstance;
        throw ue;
      }

      t.reset();
      const RequesterAndGroupMountPolicies mountPolicies = getMountPolicies(conn, diskInstanceName, user.name,
        user.group);
       const auto getMountPoliciesTime = t.secs(utils::Timer::resetCounter);

      log::ScopedParamContainer spc(lc);
      spc.add("getConnTime", getConnTime)
         .add("getArchiveFileTime", getArchiveFileTime)
         .add("getMountPoliciesTime", getMountPoliciesTime);
      lc.log(log::INFO, "Catalogue::prepareToRetrieve internal timings");

      // Requester mount policies overrule requester group mount policies
      common::dataStructures::MountPolicy mountPolicy;
      if(!mountPolicies.requesterMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterMountPolicies.front();
      } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
      } else {
        exception::UserError ue;
        ue.getMessage() << "Cannot retrieve file because there are no mount rules for the requester or their group:" <<
          " archiveFileId=" << archiveFileId << " path=" << archiveFile->diskFileInfo.path << " requester=" <<
          diskInstanceName << ":" << user.name << ":" << user.group;
        throw ue;
      }


      criteria.archiveFile = *archiveFile;
      criteria.mountPolicy = mountPolicy;
    }
    criteria.activitiesFairShareWeight = getCachedActivitiesWeights(diskInstanceName);
    return criteria;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
RequesterAndGroupMountPolicies RdbmsCatalogue::getMountPolicies(
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
        "MOUNT_POLICY.MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"
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
        "MOUNT_POLICY.MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"
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
      policy.maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");
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

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    return isCachedAdmin(admin);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isCachedAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isCachedAdmin(const common::dataStructures::SecurityIdentity &admin)
  const {
  try {
    auto getNonCachedValue = [&] {
      return isNonCachedAdmin(admin);
    };
    return m_isAdminCache.getCachedValue(admin, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isNonCachedAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isNonCachedAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME "
      "FROM "
        "ADMIN_USER "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", admin.username);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapesForWriting
//------------------------------------------------------------------------------
std::list<TapeForWriting> RdbmsCatalogue::getTapesForWriting(const std::string &logicalLibraryName) const {
  try {
    std::list<TapeForWriting> tapes;
    const char *const sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "TAPE.MEDIA_TYPE AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "TAPE_POOL.VO AS VO,"
        "TAPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_NAME = TAPE_POOL.TAPE_POOL_NAME "
      "WHERE "
//      "LABEL_DRIVE IS NOT NULL AND " // Set when the tape has been labelled
//      "LABEL_TIME IS NOT NULL AND "  // Set when the tape has been labelled
        "IS_DISABLED = '0' AND "
        "IS_FULL = '0' AND "
        "IS_READ_ONLY = '0' AND "
        "IS_FROM_CASTOR = '0' AND "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapeForWriting tape;
      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.tapePool = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");

      tapes.push_back(tape);
    }
    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertTapeFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertTapeFile(
  rdbms::Conn &conn,
  const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  rdbms::AutoRollback autoRollback(conn);
  try{
    {
      const time_t now = time(nullptr);
      const char *const sql =
        "INSERT INTO TAPE_FILE("
          "VID,"
          "FSEQ,"
          "BLOCK_ID,"
          "LOGICAL_SIZE_IN_BYTES,"
          "COPY_NB,"
          "CREATION_TIME,"
          "ARCHIVE_FILE_ID)"
        "VALUES("
          ":VID,"
          ":FSEQ,"
          ":BLOCK_ID,"
          ":LOGICAL_SIZE_IN_BYTES,"
          ":COPY_NB,"
          ":CREATION_TIME,"
          ":ARCHIVE_FILE_ID)";
      auto stmt = conn.createStmt(sql);

      stmt.bindString(":VID", tapeFile.vid);
      stmt.bindUint64(":FSEQ", tapeFile.fSeq);
      stmt.bindUint64(":BLOCK_ID", tapeFile.blockId);
      stmt.bindUint64(":LOGICAL_SIZE_IN_BYTES", tapeFile.fileSize);
      stmt.bindUint64(":COPY_NB", tapeFile.copyNb);
      stmt.bindUint64(":CREATION_TIME", now);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    {
      const char *const sql = 
      "UPDATE TAPE_FILE SET "
        "SUPERSEDED_BY_VID=:NEW_VID, " //VID of the new file
        "SUPERSEDED_BY_FSEQ=:NEW_FSEQ " //FSEQ of the new file
      "WHERE"
      " TAPE_FILE.ARCHIVE_FILE_ID=:ARCHIVE_FILE_ID AND"
      " TAPE_FILE.COPY_NB=:COPY_NB AND"
      " ( TAPE_FILE.VID <> :NEW_VID2 OR TAPE_FILE.FSEQ <> :NEW_FSEQ2 )";

      auto stmt = conn.createStmt(sql);
      stmt.bindString(":NEW_VID",tapeFile.vid);
      stmt.bindUint64(":NEW_FSEQ",tapeFile.fSeq);
      stmt.bindString(":NEW_VID2",tapeFile.vid);
      stmt.bindUint64(":NEW_FSEQ2",tapeFile.fSeq);
      stmt.bindUint64(":ARCHIVE_FILE_ID",archiveFileId);
      stmt.bindUint64(":COPY_NB",tapeFile.copyNb);
      stmt.executeNonQuery();
    }
    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeLastFseq
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeLastFSeq(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq) {
  try {
    threading::MutexLocker locker(m_mutex);

    const uint64_t currentValue = getTapeLastFSeq(conn, vid);
    if(lastFSeq != currentValue + 1) {
      exception::Exception ex;
      ex.getMessage() << "The last FSeq MUST be incremented by exactly one: currentValue=" << currentValue <<
        ",nextValue=" << lastFSeq;
      throw ex;
    }
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ "
      "WHERE "
        "VID=:VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeLastFSeq
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) const {
  try {
    const char *const sql =
      "SELECT "
        "LAST_FSEQ AS LAST_FSEQ "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      return rset.columnUint64("LAST_FSEQ");
    } else {
      throw exception::Exception(std::string("No such tape with vid=") + vid);
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileByArchiveId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileByArchiveFileId(
  rdbms::Conn &conn,
  const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience
        if (!rset.columnIsNull("SSBY_VID")) {
          tapeFile.supersededByVid = rset.columnString("SSBY_VID");
          tapeFile.supersededByFSeq = rset.columnUint64("SSBY_FSEQ");
        }

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByArchiveFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileToRetrieveByArchiveFileId(
  rdbms::Conn &conn, const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND "
        "TAPE.IS_DISABLED = '0' "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience
        if (!rset.columnIsNull("SSBY_VID")) {
          tapeFile.supersededByVid = rset.columnString("SSBY_VID");
          tapeFile.supersededByFSeq = rset.columnUint64("SSBY_FSEQ");
        }
        
        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedActivitiesWeights
//------------------------------------------------------------------------------
common::dataStructures::ActivitiesFairShareWeights 
RdbmsCatalogue::getCachedActivitiesWeights(const std::string& diskInstance) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getActivitiesWeights(conn, diskInstance);
    };
    return m_activitiesFairShareWeights.getCachedValue(diskInstance, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getActivitiesWeights
//------------------------------------------------------------------------------
common::dataStructures::ActivitiesFairShareWeights 
RdbmsCatalogue::getActivitiesWeights(rdbms::Conn& conn, const std::string& diskInstanceName) const {
  try {
    const char *const sql =
      "SELECT "
        "ACTIVITIES_WEIGHTS.ACTIVITY AS ACTIVITY,"
        "ACTIVITIES_WEIGHTS.WEIGHT AS WEIGHT "
      "FROM "
        "ACTIVITIES_WEIGHTS "
      "WHERE "
        "ACTIVITIES_WEIGHTS.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    auto rset = stmt.executeQuery();
    common::dataStructures::ActivitiesFairShareWeights afsw;
    afsw.diskInstance = diskInstanceName;
    while (rset.next()) {
      // The weight is a string encoded double with values in [0, 1], like in FTS.
      // All the checks are performed in setWeightFromString().
      afsw.setWeightFromString(rset.columnString("ACTIVITY"), rset.columnString("WEIGHT"));
    }
    return afsw;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileByDiskFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileByDiskFileId(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience
        if (!rset.columnIsNull("SSBY_VID")) {
          tapeFile.supersededByVid = rset.columnString("SSBY_VID");
          tapeFile.supersededByFSeq = rset.columnUint64("SSBY_FSEQ");
        }
        
        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByDiskFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileToRetrieveByDiskFileId(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID AND "
        "TAPE.IS_DISABLED = '0' "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience
        if (!rset.columnIsNull("SSBY_VID")) {
          tapeFile.supersededByVid = rset.columnString("SSBY_VID");
          tapeFile.supersededByFSeq = rset.columnUint64("SSBY_FSEQ");
        }
        
        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void RdbmsCatalogue::ping() {
  try {
    const char *const sql = "SELECT COUNT(*) FROM CTA_CATALOGUE";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getSchemaVersion
//------------------------------------------------------------------------------
std::map<std::string, uint64_t> RdbmsCatalogue::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t> schemaVersion;
    const char *const sql =
      "SELECT "
        "CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR "
      "FROM "
        "CTA_CATALOGUE";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    
    if(rset.next()) {
      schemaVersion.insert(std::make_pair("SCHEMA_VERSION_MAJOR", rset.columnUint64("SCHEMA_VERSION_MAJOR")));
      schemaVersion.insert(std::make_pair("SCHEMA_VERSION_MINOR", rset.columnUint64("SCHEMA_VERSION_MINOR")));
      return schemaVersion;
    } else {
      throw exception::Exception("SCHEMA_VERSION_MAJOR,SCHEMA_VERSION_MINOR not found in the CTA_CATALOGUE");
    } 
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> RdbmsCatalogue::getTableNames() const {
  auto conn = m_connPool.getConn();
  return conn.getTableNames();
}

//------------------------------------------------------------------------------
// checkTapeFileWrittenFieldsAreSet
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileWrittenFieldsAreSet(const std::string &callingFunc, const TapeFileWritten &event)
  const {
  try {
    if(event.diskInstance.empty()) throw exception::Exception("diskInstance is an empty string");
    if(event.diskFileId.empty()) throw exception::Exception("diskFileId is an empty string");
    if(event.diskFilePath.empty()) throw exception::Exception("diskFilePath is an empty string");
    if(0 == event.diskFileOwnerUid) throw exception::Exception("diskFileOwnerUid is 0");
    if(0 == event.diskFileGid) throw exception::Exception("diskFileGid is 0");
    if(0 == event.size) throw exception::Exception("size is 0");
    if(event.checksumBlob.length() == 0) throw exception::Exception("checksumBlob is an empty string");
    if(event.storageClassName.empty()) throw exception::Exception("storageClassName is an empty string");
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(0 == event.blockId && event.fSeq != 1) throw exception::Exception("blockId is 0 and fSeq is not 1");
    if(0 == event.size) throw exception::Exception("size is 0");
    if(0 == event.copyNb) throw exception::Exception("copyNb is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeFileWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// checkTapeItemWrittenFieldsAreSet
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeItemWrittenFieldsAreSet(const std::string& callingFunc, const TapeItemWritten& event) const {
  try {
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeItemWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getNbTapesInPool
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbTapesInPool(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_TAPES "
      "FROM "
        "TAPE "
      "WHERE "
        "TAPE.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset.columnUint64("NB_TAPES");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isSetAndEmpty
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isSetAndEmpty(const optional<std::string> &optionalStr) const {
  return optionalStr && optionalStr->empty();
}

} // namespace catalogue
} // namespace cta
