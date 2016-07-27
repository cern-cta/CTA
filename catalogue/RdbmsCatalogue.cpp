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
#include "catalogue/RdbmsCatalogueSchema.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"

#include <ctype.h>
#include <iostream>
#include <memory>
#include <time.h>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::RdbmsCatalogue(std::unique_ptr<rdbms::ConnFactory> connFactory, const uint64_t nbConns):
  m_connFactory(std::move(connFactory)),
  m_connPool(*m_connFactory, nbConns) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogue::~RdbmsCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void RdbmsCatalogue::createBootstrapAdminAndHostNoAuth(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &username,
  const std::string &hostName,
  const std::string &comment) {
  try {
    createAdminUser(cliIdentity, username, comment);
    createAdminHost(cliIdentity, hostName, comment);
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &username,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(adminUserExists(*conn, username)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":ADMIN_USER_NAME", username);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
  } catch(exception::UserError &) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":ADMIN_USER_NAME", adminUsername);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":ADMIN_USER_NAME", username);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-user ") + username + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<common::dataStructures::AdminUser>
RdbmsCatalogue::getAdminUsers() const {
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
        "ADMIN_USER";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::AdminUser admin;

      admin.name = rset->columnText("ADMIN_USER_NAME");
      admin.comment = rset->columnText("USER_COMMENT");
      admin.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      admin.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      admin.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      admin.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      admin.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      admin.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      admins.push_back(admin);
    }

    return admins;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &cliIdentity,
                                            const std::string &username, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void RdbmsCatalogue::createAdminHost(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &hostName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(adminHostExists(*conn, hostName)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":ADMIN_HOST_NAME", hostName);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":ADMIN_HOST_NAME", adminHost);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":ADMIN_HOST_NAME", hostName);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-host ") + hostName + " because it does not exist");
    }
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
        "ADMIN_HOST";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::AdminHost host;

      host.name = rset->columnText("ADMIN_HOST_NAME");
      host.comment = rset->columnText("USER_COMMENT");
      host.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      host.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      host.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      host.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      host.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      host.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      hosts.push_back(host);
    }

    return hosts;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminHostComment(const common::dataStructures::SecurityIdentity &cliIdentity,
                                            const std::string &hostName, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const common::dataStructures::StorageClass &storageClass) {
  try {
    auto conn = m_connPool.getConn();
    if(storageClassExists(*conn, storageClass.diskInstance, storageClass.name)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":DISK_INSTANCE_NAME", storageClass.diskInstance);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClass.name);
    stmt->bindUint64(":NB_COPIES", storageClass.nbCopies);

    stmt->bindString(":USER_COMMENT", storageClass.comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);

    stmt->executeNonQuery();
    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete storage-class ") + diskInstanceName + ":" +
        storageClassName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<common::dataStructures::StorageClass>
  RdbmsCatalogue::getStorageClasses() const {
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
        "STORAGE_CLASS";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::StorageClass storageClass;

      storageClass.diskInstance = rset->columnText("DISK_INSTANCE_NAME");
      storageClass.name = rset->columnText("STORAGE_CLASS_NAME");
      storageClass.nbCopies = rset->columnUint64("NB_COPIES");
      storageClass.comment = rset->columnText("USER_COMMENT");
      storageClass.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      storageClass.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      storageClass.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      storageClass.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      storageClass.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      storageClass.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      storageClasses.push_back(storageClass);
    }

    return storageClasses;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &name, const uint64_t nbCopies) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTapePool(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t nbPartialTapes,
  const bool encryptionValue,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();

    if(tapePoolExists(*conn, name)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":TAPE_POOL_NAME", name);
    stmt->bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt->bindUint64(":IS_ENCRYPTED", encryptionValue ? 1 : 0);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":TAPE_POOL_NAME", tapePoolName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":TAPE_POOL_NAME", name);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete tape-pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<common::dataStructures::TapePool> RdbmsCatalogue::getTapePools() const {
  try {
    std::list<common::dataStructures::TapePool> pools;
    const char *const sql =
      "SELECT "
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
        "IS_ENCRYPTED AS IS_ENCRYPTED,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE_POOL";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::TapePool pool;

      pool.name = rset->columnText("TAPE_POOL_NAME");
      pool.nbPartialTapes = rset->columnUint64("NB_PARTIAL_TAPES");
      pool.encryption = rset->columnUint64("IS_ENCRYPTED");
      pool.comment = rset->columnText("USER_COMMENT");
      pool.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      pool.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      pool.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      pool.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      pool.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      pool.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      pools.push_back(pool);
    }

    return pools;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::createArchiveRoute(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &diskInstanceName,
  const std::string &storageClassName,
  const uint64_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  try {
    const time_t now = time(nullptr);
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
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt->bindUint64(":COPY_NB", copyNb);
    stmt->bindString(":TAPE_POOL_NAME", tapePoolName);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt->bindUint64(":COPY_NB", copyNb);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot delete archive route for storage-class " << diskInstanceName + ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
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
        "ARCHIVE_ROUTE";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::ArchiveRoute route;

      route.diskInstanceName = rset->columnText("DISK_INSTANCE_NAME");
      route.storageClassName = rset->columnText("STORAGE_CLASS_NAME");
      route.copyNb = rset->columnUint64("COPY_NB");
      route.tapePoolName = rset->columnText("TAPE_POOL_NAME");
      route.comment = rset->columnText("USER_COMMENT");
      route.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      route.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      route.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      route.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      route.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      route.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      routes.push_back(route);
    }

    return routes;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(logicalLibraryExists(*conn, name)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":LOGICAL_LIBRARY_NAME", name);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete logical-library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<common::dataStructures::LogicalLibrary>
  RdbmsCatalogue::getLogicalLibraries() const {
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
        "LOGICAL_LIBRARY";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::LogicalLibrary lib;

      lib.name = rset->columnText("LOGICAL_LIBRARY_NAME");
      lib.comment = rset->columnText("USER_COMMENT");
      lib.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      lib.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      lib.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      lib.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      lib.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      lib.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      libs.push_back(lib);
    }

    return libs;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTape(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const std::string &encryptionKey,
  const uint64_t capacityInBytes,
  const bool disabled,
  const bool full,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(tapeExists(*conn, vid)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid +
        " because a tape with the same volume identifier already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE("
        "VID,"
        "LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME,"
        "ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES,"
        "LAST_FSEQ,"
        "IS_DISABLED,"
        "IS_FULL,"
        "LBP_IS_ON,"

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
        ":ENCRYPTION_KEY,"
        ":CAPACITY_IN_BYTES,"
        ":DATA_IN_BYTES,"
        ":LAST_FSEQ,"
        ":IS_DISABLED,"
        ":IS_FULL,"
        ":LBP_IS_ON,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":VID", vid);
    stmt->bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt->bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt->bindString(":ENCRYPTION_KEY", encryptionKey);
    stmt->bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt->bindUint64(":DATA_IN_BYTES", 0);
    stmt->bindUint64(":LAST_FSEQ", 0);
    stmt->bindUint64(":IS_DISABLED", disabled ? 1 : 0);
    stmt->bindUint64(":IS_FULL", full ? 1 : 0);
    stmt->bindUint64(":LBP_IS_ON", 1);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":VID", vid);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":VID", vid);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete tape ") + vid + " because it does not exist");
    }
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

    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    if(searchCriteria.vid) stmt->bindString(":VID", searchCriteria.vid.value());
    if(searchCriteria.logicalLibrary) stmt->bindString(":LOGICAL_LIBRARY_NAME", searchCriteria.logicalLibrary.value());
    if(searchCriteria.tapePool) stmt->bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    if(searchCriteria.capacityInBytes) stmt->bindUint64(":CAPACITY_IN_BYTES", searchCriteria.capacityInBytes.value());
    if(searchCriteria.disabled) stmt->bindUint64(":IS_DISABLED", searchCriteria.disabled.value() ? 1 : 0);
    if(searchCriteria.full) stmt->bindUint64(":IS_FULL", searchCriteria.full.value() ? 1 : 0);
    if(searchCriteria.lbp) stmt->bindUint64(":LBP_IS_ON", searchCriteria.lbp.value() ? 1 : 0);

    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset->columnText("VID");
      tape.logicalLibraryName = rset->columnText("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset->columnText("TAPE_POOL_NAME");
      tape.encryptionKey = rset->columnOptionalText("ENCRYPTION_KEY");
      tape.capacityInBytes = rset->columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset->columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset->columnUint64("LAST_FSEQ");
      tape.disabled = rset->columnUint64("IS_DISABLED");
      tape.full = rset->columnUint64("IS_FULL");
      tape.lbp = rset->columnUint64("LBP_IS_ON");

      tape.labelLog = getTapeLogFromRset(*rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(*rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(*rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

      tape.comment = rset->columnText("USER_COMMENT");
      tape.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      tape.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      tape.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      tape.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      tape.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      tape.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    {
      uint64_t vidNb = 1;
      for(auto &vid : vids) {
        stmt->bindString(":VID" + std::to_string(vidNb), vid);
        vidNb++;
      }
    }

    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::Tape tape;

      tape.vid = rset->columnText("VID");
      tape.logicalLibraryName = rset->columnText("LOGICAL_LIBRARY_NAME");
      tape.tapePoolName = rset->columnText("TAPE_POOL_NAME");
      tape.encryptionKey = rset->columnOptionalText("ENCRYPTION_KEY");
      tape.capacityInBytes = rset->columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset->columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset->columnUint64("LAST_FSEQ");
      tape.disabled = rset->columnUint64("IS_DISABLED");
      tape.full = rset->columnUint64("IS_FULL");
      tape.lbp = rset->columnUint64("LBP_IS_ON");

      tape.labelLog = getTapeLogFromRset(*rset, "LABEL_DRIVE", "LABEL_TIME");
      tape.lastReadLog = getTapeLogFromRset(*rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
      tape.lastWriteLog = getTapeLogFromRset(*rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

      tape.comment = rset->columnText("USER_COMMENT");
      tape.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      tape.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      tape.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      tape.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      tape.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      tape.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      vidToTapeMap[tape.vid] = tape;
    }

    if(vids.size() != vidToTapeMap.size()) {
      throw exception::Exception("Not all tapes were found");
    }

    return vidToTapeMap;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTapeLogFromRset
//------------------------------------------------------------------------------
optional<common::dataStructures::TapeLog> RdbmsCatalogue::getTapeLogFromRset(const rdbms::Rset &rset,
  const std::string &driveColName, const std::string &timeColName) const {
  try {
    const optional<std::string> drive = rset.columnOptionalText(driveColName);
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
// reclaimTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeLabelLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLabelLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeLastWrittenLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLastWrittenLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeLastReadLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLastReadLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeBusy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// setTapeLbp
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeLbp(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyRequesterMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyRequesterComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &requesterName, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyRequesterMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyRequesterComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::createMountPolicy(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t archivePriority,
  const uint64_t minArchiveRequestAge,
  const uint64_t retrievePriority,
  const uint64_t minRetrieveRequestAge,
  const uint64_t maxDrivesAllowed,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(mountPolicyExists(*conn, name)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":MOUNT_POLICY_NAME", name);

    stmt->bindUint64(":ARCHIVE_PRIORITY", archivePriority);
    stmt->bindUint64(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);

    stmt->bindUint64(":RETRIEVE_PRIORITY", retrievePriority);
    stmt->bindUint64(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);

    stmt->bindUint64(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::MountPolicy> mountPolicy(getRequesterMountPolicy(*conn, diskInstanceName,
      requesterName));
    if(nullptr != mountPolicy.get()) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName +
        " because the requester is already assigned to mount-policy " + mountPolicy->name);
    }
    if(!mountPolicyExists(*conn, mountPolicyName)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_NAME", requesterName);
    stmt->bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
        "REQUESTER_MOUNT_RULE";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while(rset->next()) {
      common::dataStructures::RequesterMountRule rule;

      rule.diskInstance = rset->columnText("DISK_INSTANCE_NAME");
      rule.name = rset->columnText("REQUESTER_NAME");
      rule.mountPolicy = rset->columnText("MOUNT_POLICY_NAME");
      rule.comment = rset->columnText("USER_COMMENT");
      rule.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_NAME", requesterName);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":" + requesterName +
        " because the rule does not exist");
    }
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
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterGroupName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::MountPolicy> mountPolicy(
      getRequesterGroupMountPolicy(*conn, diskInstanceName, requesterGroupName));
    if(nullptr != mountPolicy.get()) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester-group " + diskInstanceName + ":" + requesterGroupName +
        " because a rule already exists assigning the requester-group to mount-policy " + mountPolicy->name);
    }
    if(!mountPolicyExists(*conn, mountPolicyName)) {
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));

    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt->bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt->bindString(":USER_COMMENT", comment);

    stmt->bindString(":CREATION_LOG_USER_NAME", cliIdentity.username);
    stmt->bindString(":CREATION_LOG_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":CREATION_LOG_TIME", now);

    stmt->bindString(":LAST_UPDATE_USER_NAME", cliIdentity.username);
    stmt->bindString(":LAST_UPDATE_HOST_NAME", cliIdentity.host);
    stmt->bindUint64(":LAST_UPDATE_TIME", now);

    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    if(rset->next()) {
      std::unique_ptr<common::dataStructures::MountPolicy> policy(new common::dataStructures::MountPolicy);

      policy->name = rset->columnText("MOUNT_POLICY_NAME");

      policy->archivePriority = rset->columnUint64("ARCHIVE_PRIORITY");
      policy->archiveMinRequestAge = rset->columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy->retrievePriority = rset->columnUint64("RETRIEVE_PRIORITY");
      policy->retrieveMinRequestAge = rset->columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy->maxDrivesAllowed = rset->columnUint64("MAX_DRIVES_ALLOWED");

      policy->comment = rset->columnText("USER_COMMENT");
      policy->creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      policy->creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      policy->creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      policy->lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      policy->lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      policy->lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

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
        "REQUESTER_GROUP_MOUNT_RULE";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while(rset->next()) {
      common::dataStructures::RequesterGroupMountRule rule;

      rule.diskInstance = rset->columnText("DISK_INSTANCE_NAME");
      rule.name = rset->columnText("REQUESTER_GROUP_NAME");
      rule.mountPolicy = rset->columnText("MOUNT_POLICY_NAME");

      rule.comment = rset->columnText("USER_COMMENT");
      rule.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete the mount rule for requester group ") + diskInstanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_NAME", requesterName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_NAME", requesterName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    if(rset->next()) {
      std::unique_ptr<common::dataStructures::MountPolicy> policy(new common::dataStructures::MountPolicy);

      policy->name = rset->columnText("MOUNT_POLICY_NAME");

      policy->archivePriority = rset->columnUint64("ARCHIVE_PRIORITY");
      policy->archiveMinRequestAge = rset->columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy->retrievePriority = rset->columnUint64("RETRIEVE_PRIORITY");
      policy->retrieveMinRequestAge = rset->columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy->maxDrivesAllowed = rset->columnUint64("MAX_DRIVES_ALLOWED");

      policy->comment = rset->columnText("USER_COMMENT");

      policy->creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      policy->creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      policy->creationLog.time = rset->columnUint64("CREATION_LOG_TIME");

      common::dataStructures::EntryLog updateLog;
      policy->lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      policy->lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      policy->lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    return rset->next();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":MOUNT_POLICY_NAME", name);
    stmt->executeNonQuery();

    if(0 == stmt->getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount policy ") + name + " because it does not exist");
    }
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
        "MOUNT_POLICY";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset->columnText("MOUNT_POLICY_NAME");

      policy.archivePriority = rset->columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset->columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset->columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset->columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.maxDrivesAllowed = rset->columnUint64("MAX_DRIVES_ALLOWED");

      policy.comment = rset->columnText("USER_COMMENT");

      policy.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");

      policy.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      policies.push_back(policy);
    }

    return policies;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchivePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinFilesQueued
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchiveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinBytesQueued
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchiveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrievePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrieveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrieveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrieveMinBytesQueued) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyMaxDrivesAllowed
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyMountPolicyComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void RdbmsCatalogue::createDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteDedication(const std::string &drivename) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<common::dataStructures::Dedication> RdbmsCatalogue::getDedications() const {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationType(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationTag(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationVid(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationFrom(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationUntil(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDedicationComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
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
        "DISK_FILE_USER,"
        "DISK_FILE_GROUP,"
        "DISK_FILE_RECOVERY_BLOB,"
        "FILE_SIZE,"
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
        ":FILE_SIZE,"
        ":CHECKSUM_TYPE,"
        ":CHECKSUM_VALUE,"
        ":STORAGE_CLASS_NAME,"
        ":CREATION_TIME,"
        ":RECONCILIATION_TIME)";
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));

    stmt->bindUint64(":ARCHIVE_FILE_ID", row.archiveFileId);
    stmt->bindString(":DISK_INSTANCE_NAME", row.diskInstance);
    stmt->bindString(":DISK_FILE_ID", row.diskFileId);
    stmt->bindString(":DISK_FILE_PATH", row.diskFilePath);
    stmt->bindString(":DISK_FILE_USER", row.diskFileUser);
    stmt->bindString(":DISK_FILE_GROUP", row.diskFileGroup);
    stmt->bindString(":DISK_FILE_RECOVERY_BLOB", row.diskFileRecoveryBlob);
    stmt->bindUint64(":FILE_SIZE", row.size);
    stmt->bindString(":CHECKSUM_TYPE", row.checksumType);
    stmt->bindString(":CHECKSUM_VALUE", row.checksumValue);
    stmt->bindString(":STORAGE_CLASS_NAME", row.storageClassName);
    stmt->bindUint64(":CREATION_TIME", now);
    stmt->bindUint64(":RECONCILIATION_TIME", now);

    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFileItor
//------------------------------------------------------------------------------
std::unique_ptr<ArchiveFileItor> RdbmsCatalogue::getArchiveFileItor(const ArchiveFileSearchCriteria &searchCriteria,
  const uint64_t nbArchiveFilesToPrefetch) const {
  try {
    return std::unique_ptr<ArchiveFileItor>(new ArchiveFileItorImpl(*this, nbArchiveFilesToPrefetch, searchCriteria));
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::ArchiveFileItorImpl::ArchiveFileItorImpl(
  const RdbmsCatalogue &catalogue,
  const uint64_t nbArchiveFilesToPrefetch,
  const ArchiveFileSearchCriteria &searchCriteria):
  m_catalogue(catalogue),
  m_nbArchiveFilesToPrefetch(nbArchiveFilesToPrefetch),
  m_searchCriteria(searchCriteria),
  m_nextArchiveFileId(1) {
  try {
    if(1 > m_nbArchiveFilesToPrefetch) {
      exception::Exception ex;
      ex.getMessage() << "nbArchiveFilesToPrefetch must equal to or greater than 1: actual=" <<
        m_nbArchiveFilesToPrefetch;
      throw ex;
    }
    m_prefechedArchiveFiles = m_catalogue.getArchiveFilesForItor(m_nextArchiveFileId, m_nbArchiveFilesToPrefetch,
      m_searchCriteria);
    if(!m_prefechedArchiveFiles.empty()) {
      m_nextArchiveFileId = m_prefechedArchiveFiles.back().archiveFileID + 1;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " +ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogue::ArchiveFileItorImpl::~ArchiveFileItorImpl() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogue::ArchiveFileItorImpl::hasMore() const {
  return !m_prefechedArchiveFiles.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogue::ArchiveFileItorImpl::next() {
  try {
    if(m_prefechedArchiveFiles.empty()) {
      throw exception::Exception("No more archive files to iterate over");
    }

    common::dataStructures::ArchiveFile archiveFile = m_prefechedArchiveFiles.front();
    m_prefechedArchiveFiles.pop_front();

    if(m_prefechedArchiveFiles.empty()) {
      m_prefechedArchiveFiles = m_catalogue.getArchiveFilesForItor(m_nextArchiveFileId, m_nbArchiveFilesToPrefetch,
        m_searchCriteria);
      if(!m_prefechedArchiveFiles.empty()) {
        m_nextArchiveFileId = m_prefechedArchiveFiles.back().archiveFileID + 1;
      }
    }

    return archiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFilesForItor
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFilesForItor(
  const uint64_t startingArchiveFileId,
  const uint64_t maxNbArchiveFiles,
  const ArchiveFileSearchCriteria &searchCriteria) const {
  try {
    std::string sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_USER AS DISK_FILE_USER,"
        "ARCHIVE_FILE.DISK_FILE_GROUP AS DISK_FILE_GROUP,"
        "ARCHIVE_FILE.DISK_FILE_RECOVERY_BLOB AS DISK_FILE_RECOVERY_BLOB,"
        "ARCHIVE_FILE.FILE_SIZE AS FILE_SIZE,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE AS COMPRESSED_SIZE,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME, "
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "LEFT OUTER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID >= :STARTING_ARCHIVE_FILE_ID";
    if(searchCriteria.archiveFileId) {
      sql += " AND ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    }
    if(searchCriteria.diskInstance) {
      sql += " AND ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    }
    if(searchCriteria.diskFileId) {
      sql += " AND ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID";
    }
    if(searchCriteria.diskFilePath) {
      sql += " AND ARCHIVE_FILE.DISK_FILE_PATH = :DISK_FILE_PATH";
    }
    if(searchCriteria.diskFileUser) {
      sql += " AND ARCHIVE_FILE.DISK_FILE_USER = :DISK_FILE_USER";
    }
    if(searchCriteria.diskFileGroup) {
      sql += " AND ARCHIVE_FILE.DISK_FILE_GROUP = :DISK_FILE_GROUP";
    }
    if(searchCriteria.storageClass) {
      sql += " AND ARCHIVE_FILE.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    }
    if(searchCriteria.vid) {
      sql += " AND TAPE_FILE.VID = :VID";
    }
    if(searchCriteria.tapeFileCopyNb) {
      sql += " AND TAPE_FILE.COPY_NB = :TAPE_FILE_COPY_NB";
    }
    if(searchCriteria.tapePool) {
      sql += " AND TAPE.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    }
    sql += " ORDER BY ARCHIVE_FILE.ARCHIVE_FILE_ID, TAPE_FILE.COPY_NB";

    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindUint64(":STARTING_ARCHIVE_FILE_ID", startingArchiveFileId);
    if(searchCriteria.archiveFileId) {
      stmt->bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }
    if(searchCriteria.diskInstance) {
      stmt->bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
    }
    if(searchCriteria.diskFileId) {
      stmt->bindString(":DISK_FILE_ID", searchCriteria.diskFileId.value());
    }
    if(searchCriteria.diskFilePath) {
      stmt->bindString(":DISK_FILE_PATH", searchCriteria.diskFilePath.value());
    }
    if(searchCriteria.diskFileUser) {
      stmt->bindString(":DISK_FILE_USER", searchCriteria.diskFileUser.value());
    }
    if(searchCriteria.diskFileGroup) {
      stmt->bindString(":DISK_FILE_GROUP", searchCriteria.diskFileGroup.value());
    }
    if(searchCriteria.storageClass) {
      stmt->bindString(":STORAGE_CLASS_NAME", searchCriteria.storageClass.value());
    }
    if(searchCriteria.vid) {
      stmt->bindString(":VID", searchCriteria.vid.value());
    }
    if(searchCriteria.tapeFileCopyNb) {
      stmt->bindUint64(":TAPE_FILE_COPY_NB", searchCriteria.tapeFileCopyNb.value());
    }
    if(searchCriteria.tapePool) {
      stmt->bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    }
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    while (rset->next() && archiveFiles.size() < maxNbArchiveFiles ) {
      const uint64_t archiveFileId = rset->columnUint64("ARCHIVE_FILE_ID");

      if(archiveFiles.empty() || archiveFiles.back().archiveFileID != archiveFileId) {
        common::dataStructures::ArchiveFile archiveFile;

        archiveFile.archiveFileID = archiveFileId;
        archiveFile.diskInstance = rset->columnText("DISK_INSTANCE_NAME");
        archiveFile.diskFileId = rset->columnText("DISK_FILE_ID");
        archiveFile.diskFileInfo.path = rset->columnText("DISK_FILE_PATH");
        archiveFile.diskFileInfo.owner = rset->columnText("DISK_FILE_USER");
        archiveFile.diskFileInfo.group = rset->columnText("DISK_FILE_GROUP");
        archiveFile.diskFileInfo.recoveryBlob = rset->columnText("DISK_FILE_RECOVERY_BLOB");
        archiveFile.fileSize = rset->columnUint64("FILE_SIZE");
        archiveFile.checksumType = rset->columnText("CHECKSUM_TYPE");
        archiveFile.checksumValue = rset->columnText("CHECKSUM_VALUE");
        archiveFile.storageClass = rset->columnText("STORAGE_CLASS_NAME");
        archiveFile.creationTime = rset->columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile.reconciliationTime = rset->columnUint64("RECONCILIATION_TIME");

        archiveFiles.push_back(archiveFile);
      }

      common::dataStructures::ArchiveFile &archiveFile = archiveFiles.back();

      // If there is a tape file
      if(!rset->columnIsNull("VID")) {
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset->columnText("VID");
        tapeFile.fSeq = rset->columnUint64("FSEQ");
        tapeFile.blockId = rset->columnUint64("BLOCK_ID");
        tapeFile.compressedSize = rset->columnUint64("COMPRESSED_SIZE");
        tapeFile.copyNb = rset->columnUint64("COPY_NB");
        tapeFile.creationTime = rset->columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile.checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile.checksumValue; // Duplicated for convenience

        archiveFile.tapeFiles[rset->columnUint64("COPY_NB")] = tapeFile;
      }
    }

    return archiveFiles;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileSummary RdbmsCatalogue::getArchiveFileSummary(
  const ArchiveFileSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogue::getArchiveFileById(const uint64_t id) {
  try {
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::ArchiveFile> file(getArchiveFile(*conn, id));

    // Throw an exception if the archive file does not exist
    if(nullptr == file.get()) {
      exception::Exception ex;
      ex.getMessage() << "No such archive file with ID " << id;
      throw (ex);
    }

    return *file;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}
          
//------------------------------------------------------------------------------
// prepareForNewFile
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileQueueCriteria RdbmsCatalogue::prepareForNewFile(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::UserIdentity &user) {
  try {
    auto conn = m_connPool.getConn();
    const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getTapeCopyToPoolMap(*conn, diskInstanceName,
      storageClassName);
    const uint64_t expectedNbRoutes = getExpectedNbArchiveRoutes(*conn, diskInstanceName, storageClassName);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::Exception ex;
      ex.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " has no archive routes";
      throw ex;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::Exception ex;
      ex.getMessage() << "Storage class " << diskInstanceName << ":" << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ex;
    }

    const RequesterAndGroupMountPolicies mountPolicies = getMountPolicies(*conn, diskInstanceName, user.name,
      user.group);
    // Requester mount policies overrule requester group mount policies
    common::dataStructures::MountPolicy mountPolicy;
    if(!mountPolicies.requesterMountPolicies.empty()) {
       mountPolicy = mountPolicies.requesterMountPolicies.front();
    } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
       mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
    } else {
      throw exception::UserError(std::string("Cannot prepare for a new archive file because no mount policy exists for " +
        diskInstanceName + ":" + user.name + ":" + user.group));
    }

    // Now that we have both the archive routes and the mount policy it's safe to
    // consume an archive file identifier
    const uint64_t archiveFileId = getNextArchiveFileId(*conn);

    return common::dataStructures::ArchiveFileQueueCriteria(archiveFileId, copyToPoolMap, mountPolicy);
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      const uint64_t copyNb = rset->columnUint64("COPY_NB");
      const std::string tapePoolName = rset->columnText("TAPE_POOL_NAME");
      copyToPoolMap[copyNb] = tapePoolName;
    }

    return copyToPoolMap;
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":STORAGE_CLASS_NAME", storageClassName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    if(!rset->next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset->columnUint64("NB_ROUTES");
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::fileWrittenToTape(const TapeFileWritten &event) {
  try {
    if(event.diskInstance.empty()) throw exception::Exception("diskInstance is an empty string");
    if(event.diskFileId.empty()) throw exception::Exception("diskFileId is an empty string");
    if(event.diskFilePath.empty()) throw exception::Exception("diskFilePath is an empty string");
    if(event.diskFileUser.empty()) throw exception::Exception("diskFileUser is an empty string");
    if(event.diskFileGroup.empty()) throw exception::Exception("diskFileGroup is an empty string");
    if(event.diskFileRecoveryBlob.empty()) throw exception::Exception("diskFileRecoveryBlob is an empty string");
    if(event.checksumType.empty()) throw exception::Exception("checksumType is an empty string");
    if(event.checksumValue.empty()) throw exception::Exception("checksumValue is an empty string");
    if(event.storageClassName.empty()) throw exception::Exception("storageClassName is an empty string");
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");

    const time_t now = time(nullptr);
    std::lock_guard<std::mutex> m_lock(m_mutex);

    auto conn = m_connPool.getConn();
    const common::dataStructures::Tape tape = selectTapeForUpdate(*conn, event.vid);
    rdbms::AutoRollback autoRollback(conn.get());

    const uint64_t expectedFSeq = tape.lastFSeq + 1;
    if(expectedFSeq != event.fSeq) {
      exception::Exception ex;
      ex.getMessage() << "FSeq mismatch for tape " << event.vid << ": expected=" << expectedFSeq << " actual=" <<
        event.fSeq;
      throw ex;
    }
    updateTape(*conn, event);

    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile = getArchiveFile(*conn, event.archiveFileId);

    // If the archive file does not already exist
    if(nullptr == archiveFile.get()) {
      // Create one
      ArchiveFileRow row;
      row.archiveFileId = event.archiveFileId;
      row.diskFileId = event.diskFileId;
      row.diskInstance = event.diskInstance;
      row.size = event.size;
      row.checksumType = event.checksumType;
      row.checksumValue = event.checksumValue;
      row.storageClassName = event.storageClassName;
      row.diskFilePath = event.diskFilePath;
      row.diskFileUser = event.diskFileUser;
      row.diskFileGroup = event.diskFileGroup;
      row.diskFileRecoveryBlob = event.diskFileRecoveryBlob;
      insertArchiveFile(*conn, row);
    } else {
      throwIfCommonEventDataMismatch(*archiveFile, event);
    }

    // Create the tape file
    common::dataStructures::TapeFile tapeFile;
    tapeFile.vid            = event.vid;
    tapeFile.fSeq           = event.fSeq;
    tapeFile.blockId        = event.blockId;
    tapeFile.compressedSize = event.compressedSize;
    tapeFile.copyNb         = event.copyNb;
    tapeFile.creationTime   = now;
    insertTapeFile(*conn, tapeFile, event.archiveFileId);

    conn->commit();
    autoRollback.cancel();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::updateTape(rdbms::Conn &conn, const TapeFileWritten &event) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ,"
        "DATA_IN_BYTES = DATA_IN_BYTES + :DATA_IN_BYTES,"
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME "
      "WHERE "
        "VID=:VID";
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":VID", event.vid);
    stmt->bindUint64(":LAST_FSEQ", event.fSeq);
    stmt->bindUint64(":DATA_IN_BYTES", event.compressedSize);
    stmt->bindString(":LAST_WRITE_DRIVE", event.tapeDrive);
    stmt->bindUint64(":LAST_WRITE_TIME", now);
    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// throwIfCommonEventDataMismatch
//------------------------------------------------------------------------------
void RdbmsCatalogue::throwIfCommonEventDataMismatch(const common::dataStructures::ArchiveFile &expected,
  const TapeFileWritten &actual) const {
  // Throw an exception if the common disk information of this tape file
  // written event does not match the previous
  if(expected.diskFileId != actual.diskFileId) {
    exception::Exception ex;
    ex.getMessage() << "Disk file ID mismatch: expected=" << expected.diskFileId << " actual=" <<
    actual.diskFileId;
    throw ex;
  }
  if(expected.fileSize != actual.size) {
    exception::Exception ex;
    ex.getMessage() << "File size mismatch: expected=" << expected.fileSize << " actual=" << actual.size;
    throw ex;
  }
  if(expected.storageClass != actual.storageClassName) {
    exception::Exception ex;
    ex.getMessage() << "Storage class mismatch: expected=" << expected.storageClass << " actual=" <<
    actual.storageClassName;
    throw ex;
  }
  if(expected.diskInstance != actual.diskInstance) {
    exception::Exception ex;
    ex.getMessage() << "Disk instance mismatch: expected=" << expected.diskInstance << " actual=" <<
    actual.diskInstance;
    throw ex;
  }
  if(expected.diskFileInfo.path != actual.diskFilePath) {
    exception::Exception ex;
    ex.getMessage() << "Disk file path mismatch: expected=" << expected.diskFileInfo.path << " actual=" <<
    actual.diskFilePath;
    throw ex;
  }
  if(expected.diskFileInfo.owner != actual.diskFileUser) {
    exception::Exception ex;
    ex.getMessage() << "Disk file user mismatch: expected=" << expected.diskFileInfo.owner << " actual=" <<
    actual.diskFileUser;
    throw ex;
  }
  if(expected.diskFileInfo.group != actual.diskFileGroup) {
    exception::Exception ex;
    ex.getMessage() << "Disk file group mismatch: expected=" << expected.diskFileInfo.group << " actual=" <<
    actual.diskFileGroup;
    throw ex;
  }
  if(expected.diskFileInfo.group != actual.diskFileGroup) {
    exception::Exception ex;
    ex.getMessage() << "Disk recovery blob mismatch";
    throw ex;
  }

}

//------------------------------------------------------------------------------
// prepareToRetrieveFile
//------------------------------------------------------------------------------
common::dataStructures::RetrieveFileQueueCriteria RdbmsCatalogue::prepareToRetrieveFile(
  const std::string &diskInstanceName,
  const uint64_t archiveFileId,
  const common::dataStructures::UserIdentity &user) {
  try {
    auto conn = m_connPool.getConn();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile = getArchiveFile(*conn, archiveFileId);
    if(nullptr == archiveFile.get()) {
      exception::Exception ex;
      ex.getMessage() << "Archive file with ID " << archiveFileId << " does not exist";
      throw ex;
    }

    if(diskInstanceName != archiveFile->diskInstance) {
      exception::UserError ue;
      ue.getMessage() << "Disk instance of request does not match that of the archived file: archiveFileId=" <<
        archiveFileId << " path=" << archiveFile->diskFileInfo.path << " requestDiskInstance=" << diskInstanceName <<
        " archiveFileDiskInstance=" << archiveFile->diskInstance;
      throw ue;
    }

    const RequesterAndGroupMountPolicies mountPolicies = getMountPolicies(*conn, diskInstanceName, user.name,
      user.group);
    // Requester mount policies overrule requester group mount policies
    common::dataStructures::MountPolicy mountPolicy;
    if(!mountPolicies.requesterMountPolicies.empty()) {
      mountPolicy = mountPolicies.requesterMountPolicies.front();
    } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
      mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
    } else {
      exception::UserError ue;
      ue.getMessage() << "Cannot prepare to retrieve the file with archive file ID because no mount policy exists "
        "for " + user.name + ":" + user.group;
      throw ue;
    }

    common::dataStructures::RetrieveFileQueueCriteria criteria;
    criteria.archiveFile = *archiveFile;
    criteria.mountPolicy = mountPolicy;
    return criteria;
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

    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":REQUESTER_DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":GROUP_DISK_INSTANCE_NAME", diskInstanceName);
    stmt->bindString(":REQUESTER_NAME", requesterName);
    stmt->bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());

    RequesterAndGroupMountPolicies policies;
    while(rset->next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset->columnText("MOUNT_POLICY_NAME");
      policy.archivePriority = rset->columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset->columnUint64("ARCHIVE_MIN_REQUEST_AGE");
      policy.retrievePriority = rset->columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset->columnUint64("RETRIEVE_MIN_REQUEST_AGE");
      policy.maxDrivesAllowed = rset->columnUint64("MAX_DRIVES_ALLOWED");
      policy.comment = rset->columnText("USER_COMMENT");
      policy.creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset->columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset->columnUint64("LAST_UPDATE_TIME");

      if(rset->columnText("RULE_TYPE") == "REQUESTER") {
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
bool RdbmsCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &cliIdentity) const {
  auto conn = m_connPool.getConn();
  return userIsAdmin(*conn, cliIdentity.username) && hostIsAdmin(*conn, cliIdentity.host);
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
  std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
  stmt->bindString(":ADMIN_USER_NAME", userName);
  std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
  return rset->next();
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
  std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
  stmt->bindString(":ADMIN_HOST_NAME", hostName);
  std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
  return rset->next();
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
        "LAST_FSEQ AS LAST_FSEQ,"
        "LBP_IS_ON AS LBP_IS_ON "
      "FROM "
        "TAPE "
      "WHERE "
        "IS_DISABLED = 0 AND "
        "IS_FULL = 0 AND "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    std::unique_ptr<rdbms::Stmt> stmt(conn->createStmt(sql));
    stmt->bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    while (rset->next()) {
      TapeForWriting tape;

      tape.vid = rset->columnText("VID");
      tape.tapePool = rset->columnText("TAPE_POOL_NAME");
      tape.capacityInBytes = rset->columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset->columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset->columnUint64("LAST_FSEQ");
      tape.lbp = rset->columnUint64("LBP_IS_ON");

      tapes.push_back(tape);
    }

    return tapes;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// insertTapeFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertTapeFile(
  rdbms::Conn &conn,
  const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE_FILE("
        "VID,"
        "FSEQ,"
        "BLOCK_ID,"
        "COMPRESSED_SIZE,"
        "COPY_NB,"
        "CREATION_TIME,"
        "ARCHIVE_FILE_ID)"
      "VALUES("
        ":VID,"
        ":FSEQ,"
        ":BLOCK_ID,"
        ":COMPRESSED_SIZE,"
        ":COPY_NB,"
        ":CREATION_TIME,"
        ":ARCHIVE_FILE_ID)";
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));

    stmt->bindString(":VID", tapeFile.vid);
    stmt->bindUint64(":FSEQ", tapeFile.fSeq);
    stmt->bindUint64(":BLOCK_ID", tapeFile.blockId);
    stmt->bindUint64(":COMPRESSED_SIZE", tapeFile.compressedSize);
    stmt->bindUint64(":COPY_NB", tapeFile.copyNb);
    stmt->bindUint64(":CREATION_TIME", now);
    stmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);

    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// setTapeLastFseq
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeLastFSeq(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq) {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":VID", vid);
    stmt->bindUint64(":LAST_FSEQ", lastFSeq);
    stmt->executeNonQuery();
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
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindString(":VID", vid);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    if(rset->next()) {
      return rset->columnUint64("LAST_FSEQ");
    } else {
      throw exception::Exception(std::string("No such tape with vid=") + vid);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFile(rdbms::Conn &conn,
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
        "ARCHIVE_FILE.FILE_SIZE AS FILE_SIZE,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE AS COMPRESSED_SIZE,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    std::unique_ptr<rdbms::Stmt> stmt(conn.createStmt(sql));
    stmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    std::unique_ptr<rdbms::Rset> rset(stmt->executeQuery());
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset->next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile.reset(new common::dataStructures::ArchiveFile);

        archiveFile->archiveFileID = rset->columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset->columnText("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset->columnText("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = rset->columnText("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner = rset->columnText("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = rset->columnText("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = rset->columnText("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = rset->columnUint64("FILE_SIZE");
        archiveFile->checksumType = rset->columnText("CHECKSUM_TYPE");
        archiveFile->checksumValue = rset->columnText("CHECKSUM_VALUE");
        archiveFile->storageClass = rset->columnText("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset->columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset->columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset->columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset->columnText("VID");
        tapeFile.fSeq = rset->columnUint64("FSEQ");
        tapeFile.blockId = rset->columnUint64("BLOCK_ID");
        tapeFile.compressedSize = rset->columnUint64("COMPRESSED_SIZE");
        tapeFile.copyNb = rset->columnUint64("COPY_NB");
        tapeFile.creationTime = rset->columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[rset->columnUint64("COPY_NB")] = tapeFile;
      }
    }

    return archiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
