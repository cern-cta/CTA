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
#include "catalogue/RdbmsArchiveFileItorImpl.hpp"
#include "catalogue/RdbmsCatalogue.hpp"
#include "catalogue/retryOnLostConnection.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
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
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect):
  m_log(log),
  m_connPool(login, nbConns),
  m_archiveFileListingConnPool(login, nbArchiveFileListingConns),
  m_maxTriesToConnect(maxTriesToConnect) {
}

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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

    stmt.bindString(":ADMIN_USER_NAME", username);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":ADMIN_USER_NAME", adminUsername);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteAdminUser(const std::string &username) {
  try {
    const char *const sql = "DELETE FROM ADMIN_USER WHERE ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-user ") + username + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  try {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify admin user ") + username + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void RdbmsCatalogue::createAdminHost(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &hostName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(adminHostExists(conn, hostName)) {
      throw exception::UserError(std::string("Cannot create admin host " + hostName +
        " because an admin host with the same name already exists"));
    }
    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ADMIN_HOST("
        "ADMIN_HOST_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":ADMIN_HOST_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

    stmt.bindString(":ADMIN_HOST_NAME", hostName);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// adminHostExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::adminHostExists(rdbms::Conn &conn, const std::string adminHost) const {
  try {
    const char *const sql =
      "SELECT "
       "ADMIN_HOST_NAME AS ADMIN_HOST_NAME "
      "FROM "
        "ADMIN_HOST "
      "WHERE "
        "ADMIN_HOST_NAME = :ADMIN_HOST_NAME";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":ADMIN_HOST_NAME", adminHost);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteAdminHost(const std::string &hostName) {
  try {
    const char *const sql = "DELETE FROM ADMIN_HOST WHERE ADMIN_HOST_NAME = :ADMIN_HOST_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":ADMIN_HOST_NAME", hostName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-host ") + hostName + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<common::dataStructures::AdminHost> RdbmsCatalogue::getAdminHosts() const {
  try {
    std::list<common::dataStructures::AdminHost> hosts;
    const char *const sql =
      "SELECT "
        "ADMIN_HOST_NAME AS ADMIN_HOST_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ADMIN_HOST "
      "ORDER BY "
        "ADMIN_HOST_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::AdminHost host;

      host.name = rset.columnString("ADMIN_HOST_NAME");
      host.comment = rset.columnString("USER_COMMENT");
      host.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      host.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      host.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      host.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      host.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      host.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      hosts.push_back(host);
    }

    return hosts;
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminHostComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &hostName, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ADMIN_HOST SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "ADMIN_HOST_NAME = :ADMIN_HOST_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":ADMIN_HOST_NAME", hostName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify admin host ") + hostName + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::StorageClass &storageClass) {
  try {
    auto conn = m_connPool.getConn();
    if(storageClassExists(conn, storageClass.diskInstance, storageClass.name)) {
      throw exception::UserError(std::string("Cannot create storage class ") + storageClass.diskInstance + ":" +
        storageClass.name + " because it already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO STORAGE_CLASS("
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteStorageClass(const std::string &diskInstanceName, const std::string &storageClassName) {
  try {
    const char *const sql =
      "DELETE FROM "
        "STORAGE_CLASS "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql,rdbms::AutocommitMode::ON);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);

    stmt.executeNonQuery();
    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete storage-class ") + diskInstanceName + ":" +
        storageClassName + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTapePool(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const uint64_t nbPartialTapes,
  const bool encryptionValue,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();

    if(tapePoolExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create tape pool ") + name +
        " because a tape pool with the same name already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE_POOL("
        "TAPE_POOL_NAME,"
        "NB_PARTIAL_TAPES,"
        "IS_ENCRYPTED,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":TAPE_POOL_NAME,"
        ":NB_PARTIAL_TAPES,"
        ":IS_ENCRYPTED,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// tapePoolExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolExists(const std::string &tapePoolName) const {
  try {
    auto conn = m_connPool.getConn();
    return tapePoolExists(conn, tapePoolName);
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_PATH", diskFilePath);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// diskFileUserExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileUserExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFileUser) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_USER AS DISK_FILE_USER "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_USER = :DISK_FILE_USER";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_USER", diskFileUser);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// diskFileGroupExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFileGroup) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_GROUP AS DISK_FILE_GROUP "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_GROUP = :DISK_FILE_GROUP";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_GROUP", diskFileGroup);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// archiveRouteExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::archiveRouteExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &storageClassName, const uint64_t copyNb) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "COPY_NB AS COPY_NB "
      "FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "COPY_NB = :COPY_NB";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapePool(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete tape-pool ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,"

        "COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,"
        "COALESCE(ROUND(SUM(CAPACITY_IN_BYTES)/1000000000), 0) AS CAPACITY_IN_GB,"
        "COALESCE(ROUND(SUM(DATA_IN_BYTES)/1000000000), 0) AS DATA_IN_GB,"

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
        "TAPE_POOL.NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED,"
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapePool pool;

      pool.name = rset.columnString("TAPE_POOL_NAME");
      pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
      pool.encryption = rset.columnBool("IS_ENCRYPTED");
      pool.nbTapes = rset.columnUint64("NB_TAPES");
      pool.capacityGigabytes = rset.columnUint64("CAPACITY_IN_GB");
      pool.dataGigabytes = rset.columnUint64("DATA_IN_GB");
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  try {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::createArchiveRoute(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &diskInstanceName,
  const std::string &storageClassName,
  const uint64_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  try {
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
        "DISK_INSTANCE_NAME,"
        "STORAGE_CLASS_NAME,"
        "COPY_NB,"
        "TAPE_POOL_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"
        ":STORAGE_CLASS_NAME,"
        ":COPY_NB,"
        ":TAPE_POOL_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteArchiveRoute(const std::string &diskInstanceName, const std::string &storageClassName,
  const uint64_t copyNb) {
  try {
    const char *const sql =
      "DELETE FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "COPY_NB AS COPY_NB,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ARCHIVE_ROUTE "
      "ORDER BY "
        "DISK_INSTANCE_NAME, STORAGE_CLASS_NAME, COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb,
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
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb,
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
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
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

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":LOGICAL_LIBRARY_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(std::exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.what());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteLogicalLibrary(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM LOGICAL_LIBRARY WHERE LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete logical-library ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::LogicalLibrary lib;

      lib.name = rset.columnString("LOGICAL_LIBRARY_NAME");
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTape(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const bool disabled,
  const bool full,
  const std::string &comment) {
  try {
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
        "LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME,"
        "CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES,"
        "LAST_FSEQ,"
        "IS_DISABLED,"
        "IS_FULL,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":VID,"
        ":LOGICAL_LIBRARY_NAME,"
        ":TAPE_POOL_NAME,"
        ":CAPACITY_IN_BYTES,"
        ":DATA_IN_BYTES,"
        ":LAST_FSEQ,"
        ":IS_DISABLED,"
        ":IS_FULL,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

    stmt.bindString(":VID", vid);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt.bindUint64(":DATA_IN_BYTES", 0);
    stmt.bindUint64(":LAST_FSEQ", 0);
    stmt.bindBool(":IS_DISABLED", disabled);
    stmt.bindBool(":IS_FULL", full);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// tapeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapeExists(const std::string &vid) const {
  try {
    auto conn = m_connPool.getConn();
    return tapeExists(conn, vid);
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTape(const std::string &vid) {
  try {
    const char *const sql = "DELETE FROM TAPE WHERE VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(const TapeSearchCriteria &searchCriteria) const {
  try {
    auto conn = m_connPool.getConn();
    return getTapes(conn, searchCriteria);
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(rdbms::Conn &conn,
  const TapeSearchCriteria &searchCriteria) const {
  try {
    std::list<common::dataStructures::Tape> tapes;
    std::string sql =
      "SELECT "
        "VID AS VID,"
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ,"
        "IS_DISABLED AS IS_DISABLED,"
        "IS_FULL AS IS_FULL,"
        "LBP_IS_ON AS LBP_IS_ON,"

        "LABEL_DRIVE AS LABEL_DRIVE,"
        "LABEL_TIME AS LABEL_TIME,"

        "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "LAST_READ_TIME AS LAST_READ_TIME,"

        "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE";

    if(searchCriteria.vid||
       searchCriteria.logicalLibrary||
       searchCriteria.tapePool||
       searchCriteria.capacityInBytes||
       searchCriteria.disabled||
       searchCriteria.full||
       searchCriteria.lbp) {
      sql += " WHERE ";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.vid) {
      sql += " VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.logicalLibrary) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE_POOL_NAME = :TAPE_POOL_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.capacityInBytes) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.disabled) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " IS_DISABLED = :IS_DISABLED";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.full) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " IS_FULL = :IS_FULL";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.lbp) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " LBP_IS_ON = :LBP_IS_ON";
    }

    sql += " ORDER BY VID";

    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);

    if(searchCriteria.vid) stmt.bindString(":VID", searchCriteria.vid.value());
    if(searchCriteria.logicalLibrary) stmt.bindString(":LOGICAL_LIBRARY_NAME", searchCriteria.logicalLibrary.value());
    if(searchCriteria.tapePool) stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    if(searchCriteria.capacityInBytes) stmt.bindUint64(":CAPACITY_IN_BYTES", searchCriteria.capacityInBytes.value());
    if(searchCriteria.disabled) stmt.bindBool(":IS_DISABLED", searchCriteria.disabled.value());
    if(searchCriteria.full) stmt.bindBool(":IS_FULL", searchCriteria.full.value());
    if(searchCriteria.lbp) stmt.bindBool(":LBP_IS_ON", searchCriteria.lbp.value());

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset.columnString("VID");
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.lbp = rset.columnOptionalBool("LBP_IS_ON");

      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

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
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "VID AS VID,"
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ,"
        "IS_DISABLED AS IS_DISABLED,"
        "IS_FULL AS IS_FULL,"
        "LBP_IS_ON AS LBP_IS_ON,"

        "LABEL_DRIVE AS LABEL_DRIVE,"
        "LABEL_TIME AS LABEL_TIME,"

        "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "LAST_READ_TIME AS LAST_READ_TIME,"

        "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE";

    if(!vids.empty()) {
      sql += " WHERE ";
    }

    {
      for(uint64_t vidNb = 1; vidNb <= vids.size(); vidNb++) {
        if(1 < vidNb) {
          sql += " OR ";
        }
        sql += "VID = :VID" + std::to_string(vidNb);
      }
    }

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);

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
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.lbp = rset.columnOptionalBool("LBP_IS_ON");

      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "VID AS VID,"
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ,"
        "IS_DISABLED AS IS_DISABLED,"
        "IS_FULL AS IS_FULL,"
        "LBP_IS_ON AS LBP_IS_ON,"

        "LABEL_DRIVE AS LABEL_DRIVE,"
        "LABEL_TIME AS LABEL_TIME,"

        "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "LAST_READ_TIME AS LAST_READ_TIME,"

        "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset.columnString("VID");
      tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.disabled = rset.columnBool("IS_DISABLED");
      tape.full = rset.columnBool("IS_FULL");
      tape.lbp = rset.columnOptionalBool("LBP_IS_ON");

      tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "DATA_IN_BYTES = 0,"
        "LAST_FSEQ = 0,"
        "IS_FULL = 0,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :UPDATE_VID AND "
        "IS_FULL != 0 AND "
        "NOT EXISTS (SELECT VID FROM TAPE_FILE WHERE VID = :SELECT_VID)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":UPDATE_VID", vid);
    stmt.bindString(":SELECT_VID", vid);
    stmt.executeNonQuery();

    // If the update failed due to a user error
    if(0 == stmt.getNbAffectedRows()) {
      // Try to determine the user error
      //
      // Please note that this is a best effort diagnosis because there is no
      // lock on the database to prevent other concurrent updates from taking
      // place on the TAPE and TAPE_FILE tables
      TapeSearchCriteria searchCriteria;
      searchCriteria.vid = vid;
      const auto tapes = getTapes(conn, searchCriteria);

      if(tapes.empty()) {
        throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it does not exist");
      } else {
        if(!tapes.front().full) {
          throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it is not FULL");
        } else {
          throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because there is at least one tape"
            " file in the catalogue that is on the tape");
        }
      }
    }

    conn.commit();
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &encryptionKey) {
  try {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":ENCRYPTION_KEY", encryptionKey);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "LAST_WRITE_TIME = :LAST_WRITE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LAST_WRITE_DRIVE", drive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + "failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "LAST_READ_TIME = :LAST_READ_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LAST_READ_DRIVE", drive);
    stmt.bindUint64(":LAST_READ_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + "failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindBool(":IS_FULL", fullValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// noSpaceLeftOnTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::noSpaceLeftOnTape(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindBool(":IS_DISABLED", disabledValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::MountPolicy> mountPolicy(getRequesterMountPolicy(conn, diskInstanceName,
      requesterName));
    if(nullptr != mountPolicy.get()) {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":" + requesterName +
        " because the rule does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    std::unique_ptr<common::dataStructures::MountPolicy> mountPolicy(
      getRequesterGroupMountPolicy(conn, diskInstanceName, requesterGroupName));
    if(nullptr != mountPolicy.get()) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester-group " + diskInstanceName + ":" + requesterGroupName +
        " because a rule already exists assigning the requester-group to mount-policy " + mountPolicy->name);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);

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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getRequesterGroupMountPolicy
//------------------------------------------------------------------------------
common::dataStructures::MountPolicy *RdbmsCatalogue::getRequesterGroupMountPolicy(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterGroupName) const {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      auto policy = cta::make_unique<common::dataStructures::MountPolicy>();

      policy->name = rset.columnString("MOUNT_POLICY_NAME");

      policy->archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy->archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy->retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy->retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy->maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");

      policy->comment = rset.columnString("USER_COMMENT");
      policy->creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy->creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy->creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy->lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy->lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy->lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy.release();
    } else {
      return nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete the mount rule for requester group ") + diskInstanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getRequesterMountPolicy
//------------------------------------------------------------------------------
common::dataStructures::MountPolicy *RdbmsCatalogue::getRequesterMountPolicy(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterName) const {
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      auto policy = cta::make_unique<common::dataStructures::MountPolicy>();

      policy->name = rset.columnString("MOUNT_POLICY_NAME");

      policy->archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy->archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy->retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy->retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy->maxDrivesAllowed = rset.columnUint64("MAX_DRIVES_ALLOWED");

      policy->comment = rset.columnString("USER_COMMENT");

      policy->creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy->creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy->creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      common::dataStructures::EntryLog updateLog;
      policy->lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy->lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy->lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy.release();
    } else {
      return nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteMountPolicy(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM MOUNT_POLICY WHERE MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount policy ") + name + " because it does not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":ARCHIVE_PRIORITY", archivePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":RETRIEVE_PRIORITY", retrievePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindUint64(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// insertArchiveFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertArchiveFile(rdbms::Conn &conn, const rdbms::AutocommitMode autocommitMode,
  const ArchiveFileRow &row) {
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
      "DISK_FILE_USER,"
      "DISK_FILE_GROUP,"
      "DISK_FILE_RECOVERY_BLOB,"
      "SIZE_IN_BYTES,"
      "CHECKSUM_TYPE,"
      "CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME,"
      "CREATION_TIME,"
      "RECONCILIATION_TIME)"
    "VALUES("
      ":ARCHIVE_FILE_ID,"
      ":DISK_INSTANCE_NAME,"
      ":DISK_FILE_ID,"
      ":DISK_FILE_PATH,"
      ":DISK_FILE_USER,"
      ":DISK_FILE_GROUP,"
      ":DISK_FILE_RECOVERY_BLOB,"
      ":SIZE_IN_BYTES,"
      ":CHECKSUM_TYPE,"
      ":CHECKSUM_VALUE,"
      ":STORAGE_CLASS_NAME,"
      ":CREATION_TIME,"
      ":RECONCILIATION_TIME)";
  auto stmt = conn.createStmt(sql, autocommitMode);

  stmt.bindUint64(":ARCHIVE_FILE_ID", row.archiveFileId);
  stmt.bindString(":DISK_INSTANCE_NAME", row.diskInstance);
  stmt.bindString(":DISK_FILE_ID", row.diskFileId);
  stmt.bindString(":DISK_FILE_PATH", row.diskFilePath);
  stmt.bindString(":DISK_FILE_USER", row.diskFileUser);
  stmt.bindString(":DISK_FILE_GROUP", row.diskFileGroup);
  stmt.bindString(":DISK_FILE_RECOVERY_BLOB", row.diskFileRecoveryBlob);
  stmt.bindUint64(":SIZE_IN_BYTES", row.size);
  stmt.bindString(":CHECKSUM_TYPE", row.checksumType);
  stmt.bindString(":CHECKSUM_VALUE", row.checksumValue);
  stmt.bindString(":STORAGE_CLASS_NAME", row.storageClassName);
  stmt.bindUint64(":CREATION_TIME", now);
  stmt.bindUint64(":RECONCILIATION_TIME", now);

  stmt.executeNonQuery();
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

  if(searchCriteria.diskFileGroup && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file group ") + searchCriteria.diskFileGroup.value() + " is ambiguous "
      "without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFileGroup) {
    if(!diskFileGroupExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFileGroup.value())) {
      throw exception::UserError(std::string("Disk file group ") + searchCriteria.diskInstance.value() + "::" +
        searchCriteria.diskFileGroup.value() + " does not exist");
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

  if(searchCriteria.diskFileUser && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file user ") + searchCriteria.diskFileUser.value() + " is ambiguous "
      "without disk instance name");
  }

  if(searchCriteria.diskInstance && searchCriteria.diskFileUser) {
    if(!diskFileUserExists(conn, searchCriteria.diskInstance.value(), searchCriteria.diskFileUser.value())) {
      throw exception::UserError(std::string("Disk file user ") + searchCriteria.diskInstance.value() + "::" +
        searchCriteria.diskFileUser.value() + " does not exist");
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
// getArchiveFiles
//------------------------------------------------------------------------------
ArchiveFileItor RdbmsCatalogue::getArchiveFiles(const TapeFileSearchCriteria &searchCriteria) const {

  checkTapeFileSearchCriteria(searchCriteria);

  try {
    auto impl = new RdbmsArchiveFileItorImpl(m_log, m_archiveFileListingConnPool, searchCriteria);
    return ArchiveFileItor(impl);
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "COALESCE(SUM(TAPE_FILE.COMPRESSED_SIZE_IN_BYTES), 0) AS TOTAL_COMPRESSED_BYTES,"
        "COUNT(ARCHIVE_FILE.ARCHIVE_FILE_ID) AS TOTAL_FILES "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID";

    if(
      searchCriteria.archiveFileId  ||
      searchCriteria.diskInstance   ||
      searchCriteria.diskFileId     ||
      searchCriteria.diskFilePath   ||
      searchCriteria.diskFileUser   ||
      searchCriteria.diskFileGroup  ||
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
    if(searchCriteria.diskFileUser) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_USER = :DISK_FILE_USER";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileGroup) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_GROUP = :DISK_FILE_GROUP";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.storageClass) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
    if(searchCriteria.diskFileUser) {
      stmt.bindString(":DISK_FILE_USER", searchCriteria.diskFileUser.value());
    }
    if(searchCriteria.diskFileGroup) {
      stmt.bindString(":DISK_FILE_GROUP", searchCriteria.diskFileGroup.value());
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
    summary.totalCompressedBytes = rset.columnUint64("TOTAL_COMPRESSED_BYTES");
    summary.totalFiles = rset.columnUint64("TOTAL_FILES");
    return summary;
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// tapeLabelled
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeLabelled(const std::string &vid, const std::string &drive, const bool lbpIsOn) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LABEL_DRIVE = :LABEL_DRIVE,"
        "LABEL_TIME = :LABEL_TIME,"
        "LBP_IS_ON = :LBP_IS_ON "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":LABEL_DRIVE", drive);
    stmt.bindUint64(":LABEL_TIME", now);
    stmt.bindBool(":LBP_IS_ON", lbpIsOn);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// checkAndGetNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::UserIdentity &user) {
  try {
    auto conn = m_connPool.getConn();
    const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getTapeCopyToPoolMap(conn, diskInstanceName,
      storageClassName);
    const uint64_t expectedNbRoutes = getExpectedNbArchiveRoutes(conn, diskInstanceName, storageClassName);

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

    const RequesterAndGroupMountPolicies mountPolicies = getMountPolicies(conn, diskInstanceName, user.name,
      user.group);
    // Requester mount policies overrule requester group mount policies
    common::dataStructures::MountPolicy mountPolicy;
    if(!mountPolicies.requesterMountPolicies.empty()) {
      mountPolicy = mountPolicies.requesterMountPolicies.front();
    } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
      mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
    } else {
      exception::UserError ue;
      ue.getMessage() << "No mount rules for the requester or their group:"
        " storageClass=" << storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" <<
        user.group;
      throw ue;
    }

    // Now that we have found both the archive routes and the mount policy it's
    // safe to consume an archive file identifier
    return getNextArchiveFileId(conn);
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &ue) {
    throw exception::UserError(std::string("Cannot allocate a new archive file: ") + ue.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFileQueueCriteria
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileQueueCriteria RdbmsCatalogue::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::UserIdentity &user) {
  try {
    auto conn = m_connPool.getConn();
    const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getTapeCopyToPoolMap(conn, diskInstanceName,
      storageClassName);
    const uint64_t expectedNbRoutes = getExpectedNbArchiveRoutes(conn, diskInstanceName, storageClassName);

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

    const RequesterAndGroupMountPolicies mountPolicies = getMountPolicies(conn, diskInstanceName, user.name,
      user.group);
    // Requester mount policies overrule requester group mount policies
    common::dataStructures::MountPolicy mountPolicy;
    if(!mountPolicies.requesterMountPolicies.empty()) {
       mountPolicy = mountPolicies.requesterMountPolicies.front();
    } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
       mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
    } else {
      exception::UserError ue;
      ue.getMessage() << "Cannot archive file because there are no mount rules for the requester or their group:"
        " storageClass=" << storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" <<
        user.group;
      throw ue;
    }

    return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, mountPolicy);
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTapeCopyToPoolMap
//------------------------------------------------------------------------------
common::dataStructures::TapeCopyToPoolMap RdbmsCatalogue::getTapeCopyToPoolMap(rdbms::Conn &conn,
  const std::string &diskInstanceName, const std::string &storageClassName) const {
  try {
    common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
    const char *const sql =
      "SELECT "
        "COPY_NB AS COPY_NB,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      const uint64_t copyNb = rset.columnUint64("COPY_NB");
      const std::string tapePoolName = rset.columnString("TAPE_POOL_NAME");
      copyToPoolMap[copyNb] = tapePoolName;
    }

    return copyToPoolMap;
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getExpectedNbArchiveRoutes(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROUTES "
      "FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset.columnUint64("NB_ROUTES");
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::updateTape(
  rdbms::Conn &conn,
  const rdbms::AutocommitMode autocommitMode,
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
    auto stmt = conn.createStmt(sql, autocommitMode);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.bindUint64(":DATA_IN_BYTES", compressedBytesWritten);
    stmt.bindString(":LAST_WRITE_DRIVE", tapeDrive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// prepareToRetrieveFile
//------------------------------------------------------------------------------
common::dataStructures::RetrieveFileQueueCriteria RdbmsCatalogue::prepareToRetrieveFile(
  const std::string &diskInstanceName,
  const uint64_t archiveFileId,
  const common::dataStructures::UserIdentity &user,
  log::LogContext &lc) {
  try {
    cta::utils::Timer t;
    auto conn = m_connPool.getConn();
    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto archiveFile = getArchiveFileByArchiveFileId(conn, archiveFileId);
    const auto getArchiveFileTime = t.secs(utils::Timer::resetCounter);
    if(nullptr == archiveFile.get()) {
      exception::Exception ex;
      ex.getMessage() << "Archive file with ID " << archiveFileId << " does not exist";
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

    common::dataStructures::RetrieveFileQueueCriteria criteria;
    criteria.archiveFile = *archiveFile;
    criteria.mountPolicy = mountPolicy;
    return criteria;
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// prepareToRetrieveFileByDiskFileId
//------------------------------------------------------------------------------
common::dataStructures::RetrieveFileQueueCriteria RdbmsCatalogue::prepareToRetrieveFileByDiskFileId(
  const std::string &diskInstanceName,
  const std::string &diskFileId,
  const common::dataStructures::UserIdentity &user,
  log::LogContext &lc) {
  try {
    cta::utils::Timer t;
    auto conn = m_connPool.getConn();
    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto archiveFile = getArchiveFileByDiskFileId(conn, diskInstanceName, diskFileId);
    const auto getArchiveFileTime = t.secs(utils::Timer::resetCounter);
    if(nullptr == archiveFile.get()) {
      exception::Exception ex;
      ex.getMessage() << "Archive file with disk instance name " << diskInstanceName << " and disk file ID" <<
        diskFileId << " does not exist";
      throw ex;
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
        " diskInstanceName=" << diskInstanceName << " diskFileId=" << diskFileId << " archiveFileId=" <<
        archiveFile->archiveFileID << " path=" << archiveFile->diskFileInfo.path << " requester=" << diskInstanceName <<
        ":" << user.name << ":" << user.group;
      throw ue;
    }

    common::dataStructures::RetrieveFileQueueCriteria criteria;
    criteria.archiveFile = *archiveFile;
    criteria.mountPolicy = mountPolicy;
    return criteria;
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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

    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    auto conn = m_connPool.getConn();
    return userIsAdmin(conn, admin.username) && hostIsAdmin(conn, admin.host);
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// userIsAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::userIsAdmin(rdbms::Conn &conn, const std::string &userName) const {
  const char *const sql =
    "SELECT "
      "ADMIN_USER_NAME AS ADMIN_USER_NAME "
    "FROM "
      "ADMIN_USER "
    "WHERE "
      "ADMIN_USER_NAME = :ADMIN_USER_NAME";
  auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
  stmt.bindString(":ADMIN_USER_NAME", userName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

//------------------------------------------------------------------------------
// hostIsAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::hostIsAdmin(rdbms::Conn &conn, const std::string &hostName) const {
  const char *const sql =
    "SELECT "
      "ADMIN_HOST_NAME AS ADMIN_HOST_NAME "
    "FROM "
      "ADMIN_HOST "
    "WHERE "
      "ADMIN_HOST_NAME = :ADMIN_HOST_NAME";
  auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
  stmt.bindString(":ADMIN_HOST_NAME", hostName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

//------------------------------------------------------------------------------
// getTapesForWriting
//------------------------------------------------------------------------------
std::list<TapeForWriting> RdbmsCatalogue::getTapesForWriting(const std::string &logicalLibraryName) const {
  try {
    std::list<TapeForWriting> tapes;
    const char *const sql =
      "SELECT "
        "VID AS VID,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ "
        "FROM "
        "TAPE "
        "WHERE "
//      "LBP_IS_ON IS NOT NULL AND "   // Set when the tape has been labelled
//      "LABEL_DRIVE IS NOT NULL AND " // Set when the tape has been labelled
//      "LABEL_TIME IS NOT NULL AND "  // Set when the tape has been labelled
        "IS_DISABLED = 0 AND "
        "IS_FULL = 0 AND "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapeForWriting tape;
      tape.vid = rset.columnString("VID");
      tape.tapePool = rset.columnString("TAPE_POOL_NAME");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");

      tapes.push_back(tape);
    }
    return tapes;
  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// insertTapeFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertTapeFile(
  rdbms::Conn &conn,
  const rdbms::AutocommitMode autocommitMode,
  const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE_FILE("
        "VID,"
        "FSEQ,"
        "BLOCK_ID,"
        "COMPRESSED_SIZE_IN_BYTES,"
        "COPY_NB,"
        "CREATION_TIME,"
        "ARCHIVE_FILE_ID)"
      "VALUES("
        ":VID,"
        ":FSEQ,"
        ":BLOCK_ID,"
        ":COMPRESSED_SIZE_IN_BYTES,"
        ":COPY_NB,"
        ":CREATION_TIME,"
        ":ARCHIVE_FILE_ID)";
    auto stmt = conn.createStmt(sql, autocommitMode);

    stmt.bindString(":VID", tapeFile.vid);
    stmt.bindUint64(":FSEQ", tapeFile.fSeq);
    stmt.bindUint64(":BLOCK_ID", tapeFile.blockId);
    stmt.bindUint64(":COMPRESSED_SIZE_IN_BYTES", tapeFile.compressedSize);
    stmt.bindUint64(":COPY_NB", tapeFile.copyNb);
    stmt.bindUint64(":CREATION_TIME", now);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);

    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      return rset.columnUint64("LAST_FSEQ");
    } else {
      throw exception::Exception(std::string("No such tape with vid=") + vid);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "ARCHIVE_FILE.DISK_FILE_USER AS DISK_FILE_USER,"
        "ARCHIVE_FILE.DISK_FILE_GROUP AS DISK_FILE_GROUP,"
        "ARCHIVE_FILE.DISK_FILE_RECOVERY_BLOB AS DISK_FILE_RECOVERY_BLOB,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE_IN_BYTES AS COMPRESSED_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
        archiveFile->diskFileInfo.owner = rset.columnString("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = rset.columnString("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = rset.columnString("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumType = rset.columnString("CHECKSUM_TYPE");
        archiveFile->checksumValue = rset.columnString("CHECKSUM_VALUE");
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
        tapeFile.compressedSize = rset.columnUint64("COMPRESSED_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[rset.columnUint64("COPY_NB")] = tapeFile;
      }
    }

    return archiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
        "ARCHIVE_FILE.DISK_FILE_USER AS DISK_FILE_USER,"
        "ARCHIVE_FILE.DISK_FILE_GROUP AS DISK_FILE_GROUP,"
        "ARCHIVE_FILE.DISK_FILE_RECOVERY_BLOB AS DISK_FILE_RECOVERY_BLOB,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE_IN_BYTES AS COMPRESSED_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
        archiveFile->diskFileInfo.owner = rset.columnString("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = rset.columnString("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = rset.columnString("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumType = rset.columnString("CHECKSUM_TYPE");
        archiveFile->checksumValue = rset.columnString("CHECKSUM_VALUE");
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
        tapeFile.compressedSize = rset.columnUint64("COMPRESSED_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[rset.columnUint64("COPY_NB")] = tapeFile;
      }
    }

    return archiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void RdbmsCatalogue::ping() {
  const char *const sql = "SELECT COUNT(*) FROM CTA_CATALOGUE";
  auto conn = m_connPool.getConn();
  auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
  auto rset = stmt.executeQuery();
}

//------------------------------------------------------------------------------
// checkTapeWrittenFilesAreSet
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileWrittenFieldsAreSet(const std::string &callingFunc, const TapeFileWritten &event)
  const {
  try {
    if(event.diskInstance.empty()) throw exception::Exception("diskInstance is an empty string");
    if(event.diskFileId.empty()) throw exception::Exception("diskFileId is an empty string");
    if(event.diskFilePath.empty()) throw exception::Exception("diskFilePath is an empty string");
    if(event.diskFileUser.empty()) throw exception::Exception("diskFileUser is an empty string");
    if(event.diskFileGroup.empty()) throw exception::Exception("diskFileGroup is an empty string");
    if(event.diskFileRecoveryBlob.empty()) throw exception::Exception("diskFileRecoveryBlob is an empty string");
    if(0 == event.size) throw exception::Exception("size is 0");
    if(event.checksumType.empty()) throw exception::Exception("checksumType is an empty string");
    if(event.checksumValue.empty()) throw exception::Exception("checksumValue is an empty string");
    if(event.storageClassName.empty()) throw exception::Exception("storageClassName is an empty string");
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(0 == event.blockId && event.fSeq != 1) throw exception::Exception("blockId is 0 and fSeq is not 1");
    if(0 == event.compressedSize) throw exception::Exception("compressedSize is 0");
    if(0 == event.copyNb) throw exception::Exception("copyNb is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch(exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeFileWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
