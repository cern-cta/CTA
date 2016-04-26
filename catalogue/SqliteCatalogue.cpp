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

#include "catalogue/SqliteCatalogue.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/SqliteStmt.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/exception/Exception.hpp"

#include <memory>
#include <sqlite3.h>
#include <time.h>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue():
  m_conn(":memory:"),
  m_nextArchiveFileId(1) {
  createDbSchema();
}

//------------------------------------------------------------------------------
// createSchema
//------------------------------------------------------------------------------
void SqliteCatalogue::createDbSchema() {
  const SqliteCatalogueSchema schema;
  m_conn.enableForeignKeys();
  m_conn.execNonQuery(schema.sql);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteCatalogue::~SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void SqliteCatalogue::createBootstrapAdminAndHostNoAuth(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const common::dataStructures::UserIdentity &user,
  const std::string &hostName,
  const std::string &comment) {
  createAdminUser(cliIdentity, user, comment);
  createAdminHost(cliIdentity, hostName, comment);
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void SqliteCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const common::dataStructures::UserIdentity &user,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ADMIN_USER("
      "ADMIN_USER_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":ADMIN_USER_NAME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":ADMIN_USER_NAME", user.name);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteAdminUser(const common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser>
  SqliteCatalogue::getAdminUsers() const {
  std::list<common::dataStructures::AdminUser> admins;
  const char *const sql =
    "SELECT "
      "ADMIN_USER_NAME AS ADMIN_USER_NAME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM ADMIN_USER;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::AdminUser admin;

    admin.name = stmt->columnText(nameToIdx["ADMIN_USER_NAME"]);

    admin.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    admin.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    admin.lastModificationLog = updateLog;

    admins.push_back(admin);
  }

  return admins;
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void SqliteCatalogue::createAdminHost(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &hostName,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ADMIN_HOST("
      "ADMIN_HOST_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":ADMIN_HOST_NAME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":ADMIN_HOST_NAME", hostName);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteAdminHost(const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> SqliteCatalogue::getAdminHosts() const {
  std::list<common::dataStructures::AdminHost> hosts;
  const char *const sql =
    "SELECT "
      "ADMIN_HOST_NAME AS ADMIN_HOST_NAME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM ADMIN_HOST;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::AdminHost host;

    host.name = stmt->columnText(nameToIdx["ADMIN_HOST_NAME"]);
    host.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    host.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    host.lastModificationLog = updateLog;

    hosts.push_back(host);
  }

  return hosts;
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyAdminHostComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void SqliteCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t nbCopies,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO STORAGE_CLASS("
      "STORAGE_CLASS_NAME,"
      "NB_COPIES,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":STORAGE_CLASS_NAME,"
      ":NB_COPIES,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":STORAGE_CLASS_NAME", name);
  stmt->bind(":NB_COPIES", nbCopies);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteStorageClass(const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass>
  SqliteCatalogue::getStorageClasses() const {
  std::list<common::dataStructures::StorageClass> storageClasses;
  const char *const sql =
    "SELECT "
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "NB_COPIES          AS NB_COPIES,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM STORAGE_CLASS;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::StorageClass storageClass;

    storageClass.name = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);
    storageClass.nbCopies = stmt->columnUint64(nameToIdx["NB_COPIES"]);
    storageClass.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    storageClass.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    storageClass.lastModificationLog = updateLog;

    storageClasses.push_back(storageClass);
  }

  return storageClasses;
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) {}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void SqliteCatalogue::createTapePool(
  const cta::common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t nbPartialTapes,
  const bool encryptionValue,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO TAPE_POOL("
      "TAPE_POOL_NAME,"
      "NB_PARTIAL_TAPES,"
      "IS_ENCRYPTED,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":TAPE_POOL_NAME,"
      ":NB_PARTIAL_TAPES,"
      ":IS_ENCRYPTED,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":TAPE_POOL_NAME", name);
  stmt->bind(":NB_PARTIAL_TAPES", nbPartialTapes);
  stmt->bind(":IS_ENCRYPTED", encryptionValue);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteTapePool(const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool>
  SqliteCatalogue::getTapePools() const {
  std::list<cta::common::dataStructures::TapePool> pools;
  const char *const sql =
    "SELECT "
      "TAPE_POOL_NAME   AS TAPE_POOL_NAME,"
      "NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
      "IS_ENCRYPTED     AS IS_ENCRYPTED,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM TAPE_POOL;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::TapePool pool;

    pool.name = stmt->columnText(nameToIdx["TAPE_POOL_NAME"]);
    pool.nbPartialTapes = stmt->columnUint64(nameToIdx["NB_PARTIAL_TAPES"]);
    pool.encryption = stmt->columnUint64(nameToIdx["IS_ENCRYPTED"]);

    pool.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    pool.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    pool.lastModificationLog = updateLog;

    pools.push_back(pool);
  }

  return pools;
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) {}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapePoolEncryption(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) {}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void SqliteCatalogue::createArchiveRoute(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &storageClassName,
  const uint64_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ARCHIVE_ROUTE("
      "STORAGE_CLASS_NAME,"
      "COPY_NB,"
      "TAPE_POOL_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":STORAGE_CLASS_NAME,"
      ":COPY_NB,"
      ":TAPE_POOL_NAME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":STORAGE_CLASS_NAME", storageClassName);
  stmt->bind(":COPY_NB", copyNb);
  stmt->bind(":TAPE_POOL_NAME", tapePoolName);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute>
  SqliteCatalogue::getArchiveRoutes() const {
  std::list<common::dataStructures::ArchiveRoute> routes;
  const char *const sql =
    "SELECT "
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "COPY_NB            AS COPY_NB,"
      "TAPE_POOL_NAME     AS TAPE_POOL_NAME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM ARCHIVE_ROUTE;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::ArchiveRoute route;

    route.storageClassName = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);
    route.copyNb = stmt->columnUint64(nameToIdx["COPY_NB"]);
    route.tapePoolName = stmt->columnText(nameToIdx["TAPE_POOL_NAME"]);

    route.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    route.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    route.lastModificationLog = updateLog;

    routes.push_back(route);
  }

  return routes;
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void SqliteCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO LOGICAL_LIBRARY("
      "LOGICAL_LIBRARY_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":LOGICAL_LIBRARY_NAME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":LOGICAL_LIBRARY_NAME", name);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteLogicalLibrary(const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary>
  SqliteCatalogue::getLogicalLibraries() const {
  std::list<cta::common::dataStructures::LogicalLibrary> libs;
  const char *const sql =
    "SELECT "
      "LOGICAL_LIBRARY_NAME      AS LOGICAL_LIBRARY_NAME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM LOGICAL_LIBRARY;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::LogicalLibrary lib;

    lib.name = stmt->columnText(nameToIdx["LOGICAL_LIBRARY_NAME"]);

    lib.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    lib.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    lib.lastModificationLog = updateLog;

    libs.push_back(lib);
  }

  return libs;
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void SqliteCatalogue::createTape(
  const cta::common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const std::string &encryptionKey,
  const uint64_t capacityInBytes,
  const bool disabledValue,
  const bool fullValue,
  const std::string &comment) {
  const time_t now = time(NULL);
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

      "LABEL_DRIVE,"
      "LABEL_TIME,"

      "LAST_READ_DRIVE,"
      "LAST_READ_TIME,"

      "LAST_WRITE_DRIVE,"
      "LAST_WRITE_TIME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
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

      ":LABEL_DRIVE,"
      ":LABEL_TIME,"

      ":LAST_READ_DRIVE,"
      ":LAST_READ_TIME,"

      ":LAST_WRITE_DRIVE,"
      ":LAST_WRITE_TIME,"

      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":VID", vid);
  stmt->bind(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
  stmt->bind(":TAPE_POOL_NAME", tapePoolName);
  stmt->bind(":ENCRYPTION_KEY", encryptionKey);
  stmt->bind(":CAPACITY_IN_BYTES", capacityInBytes);
  stmt->bind(":DATA_IN_BYTES", 0);
  stmt->bind(":LAST_FSEQ", 0);
  stmt->bind(":IS_DISABLED", disabledValue);
  stmt->bind(":IS_FULL", fullValue);
  stmt->bind(":LBP_IS_ON", 1);

  stmt->bind(":LABEL_DRIVE", "");
  stmt->bind(":LABEL_TIME", 0);

  stmt->bind(":LAST_READ_DRIVE", "");
  stmt->bind(":LAST_READ_TIME", 0);

  stmt->bind(":LAST_WRITE_DRIVE", "");
  stmt->bind(":LAST_WRITE_TIME", 0);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteTape(const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape>
  SqliteCatalogue::getTapes(
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const std::string &capacityInBytes,
  const std::string &disabledValue,
  const std::string &fullValue,
  const std::string &busyValue,
  const std::string &lbpValue) {
  std::list<cta::common::dataStructures::Tape> tapes;
  const char *const sql =
    "SELECT "
      "VID                  AS VID,"
      "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
      "TAPE_POOL_NAME       AS TAPE_POOL_NAME,"
      "ENCRYPTION_KEY       AS ENCRYPTION_KEY,"
      "CAPACITY_IN_BYTES    AS CAPACITY_IN_BYTES,"
      "DATA_IN_BYTES        AS DATA_IN_BYTES,"
      "LAST_FSEQ            AS LAST_FSEQ,"
      "IS_DISABLED          AS IS_DISABLED,"
      "IS_FULL              AS IS_FULL,"
      "LBP_IS_ON            AS LBP_IS_ON,"

      "LABEL_DRIVE AS LABEL_DRIVE,"
      "LABEL_TIME  AS LABEL_TIME,"

      "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "LAST_READ_TIME  AS LAST_READ_TIME,"

      "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "LAST_WRITE_TIME  AS LAST_WRITE_TIME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM TAPE;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::Tape tape;

    tape.vid = stmt->columnText(nameToIdx["VID"]);
    tape.logicalLibraryName =
      stmt->columnText(nameToIdx["LOGICAL_LIBRARY_NAME"]);
    tape.tapePoolName = stmt->columnText(nameToIdx["TAPE_POOL_NAME"]);
    tape.encryptionKey = stmt->columnText(nameToIdx["ENCRYPTION_KEY"]);
    tape.capacityInBytes = stmt->columnUint64(nameToIdx["CAPACITY_IN_BYTES"]);
    tape.dataOnTapeInBytes = stmt->columnUint64(nameToIdx["DATA_IN_BYTES"]);
    tape.lastFSeq = stmt->columnUint64(nameToIdx["LAST_FSEQ"]);
    tape.disabled = stmt->columnUint64(nameToIdx["IS_DISABLED"]);
    tape.full = stmt->columnUint64(nameToIdx["IS_FULL"]);
    tape.lbp = stmt->columnUint64(nameToIdx["LBP_IS_ON"]);

    tape.labelLog.drive = stmt->columnText(nameToIdx["LABEL_DRIVE"]);
    tape.labelLog.time = stmt->columnUint64(nameToIdx["LABEL_TIME"]);

    tape.lastReadLog.drive = stmt->columnText(nameToIdx["LAST_READ_DRIVE"]);
    tape.lastReadLog.time = stmt->columnUint64(nameToIdx["LAST_READ_TIME"]);

    tape.lastWriteLog.drive = stmt->columnText(nameToIdx["LAST_WRITE_DRIVE"]);
    tape.lastWriteLog.time = stmt->columnUint64(nameToIdx["LAST_WRITE_TIME"]);

    tape.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    tape.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    tape.lastModificationLog = updateLog;

    tapes.push_back(tape);
  }

  return tapes;
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void SqliteCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) {}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) {}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeEncryptionKey(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) {}

//------------------------------------------------------------------------------
// modifyTapeLabelLog
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeLabelLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastWrittenLog
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeLastWrittenLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastReadLog
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeLastReadLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapeBusy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) {}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) {}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) {}

//------------------------------------------------------------------------------
// setTapeLbp
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) {}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) {}

//------------------------------------------------------------------------------
// createRequester
//------------------------------------------------------------------------------
void SqliteCatalogue::createRequester(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const cta::common::dataStructures::UserIdentity &user,
  const std::string &mountPolicy,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO REQUESTER("
      "REQUESTER_NAME,"
      "MOUNT_POLICY_NAME,"

      "USER_COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME)"
    "VALUES("
      ":REQUESTER_NAME,"
      ":MOUNT_POLICY_NAME,"
  
      ":USER_COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":REQUESTER_NAME", user.name);
  stmt->bind(":MOUNT_POLICY_NAME", mountPolicy);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteRequester
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteRequester(const cta::common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getRequesters
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Requester>
  SqliteCatalogue::getRequesters() const {
  std::list<common::dataStructures::Requester> users;
  const char *const sql =
    "SELECT "
      "REQUESTER_NAME    AS REQUESTER_NAME,"
      "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM REQUESTER;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::Requester user;
    
    user.name = stmt->columnText(nameToIdx["REQUESTER_NAME"]);
    user.group = "N/A";
    user.mountPolicy = stmt->columnText(nameToIdx["MOUNT_POLICY_NAME"]);

    user.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    user.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    user.lastModificationLog = updateLog;

    users.push_back(user);
  }

  return users;
}

//------------------------------------------------------------------------------
// modifyRequesterMountPolicy
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyRequesterMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &mountPolicy) {}

//------------------------------------------------------------------------------
// modifyRequesterComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyRequesterComment(const common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createMountPolicy
//------------------------------------------------------------------------------
void SqliteCatalogue::createMountPolicy(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t archivePriority,
  const uint64_t minArchiveRequestAge,
  const uint64_t retrievePriority,
  const uint64_t minRetrieveRequestAge,
  const uint64_t maxDrivesAllowed,
  const std::string &comment) {
  const time_t now = time(NULL);
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
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME,"
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
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":MOUNT_POLICY_NAME", name);

  stmt->bind(":ARCHIVE_PRIORITY", archivePriority);
  stmt->bind(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);

  stmt->bind(":RETRIEVE_PRIORITY", retrievePriority);
  stmt->bind(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);

  stmt->bind(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);

  stmt->bind(":USER_COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteMountPolicy
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteMountPolicy(const std::string &name) {}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::MountPolicy>
  SqliteCatalogue::getMountPolicies() const {
  std::list<cta::common::dataStructures::MountPolicy> policies;
  const char *const sql =
    "SELECT "
      "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

      "ARCHIVE_PRIORITY        AS ARCHIVE_PRIORITY,"
      "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

      "RETRIEVE_PRIORITY        AS RETRIEVE_PRIORITY,"
      "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

      "MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

      "USER_COMMENT AS USER_COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM MOUNT_POLICY;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::MountPolicy policy;

    policy.name = stmt->columnText(nameToIdx["MOUNT_POLICY_NAME"]);

    policy.archive_priority = stmt->columnUint64(nameToIdx["ARCHIVE_PRIORITY"]);
    policy.archive_minRequestAge =
      stmt->columnUint64(nameToIdx["ARCHIVE_MIN_REQUEST_AGE"]);

    policy.retrieve_priority =
      stmt->columnUint64(nameToIdx["RETRIEVE_PRIORITY"]);
    policy.retrieve_minRequestAge =
      stmt->columnUint64(nameToIdx["RETRIEVE_MIN_REQUEST_AGE"]);

    policy.maxDrivesAllowed =
      stmt->columnUint64(nameToIdx["MAX_DRIVES_ALLOWED"]);

    policy.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    policy.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    policy.lastModificationLog = updateLog;

    policies.push_back(policy);
  }

  return policies;
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchivePriority
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) {}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinFilesQueued
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyArchiveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinBytesQueued
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyArchiveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinRequestAge
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrievePriority
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) {}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyRetrieveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyRetrieveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrieveMinBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinRequestAge
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountPolicyMaxDrivesAllowed
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) {}

//------------------------------------------------------------------------------
// modifyMountPolicyComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void SqliteCatalogue::createDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteDedication(const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> SqliteCatalogue::getDedications() const { return std::list<cta::common::dataStructures::Dedication>();}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationType(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationTag(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) {}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationVid(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationFrom(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationUntil(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void SqliteCatalogue::modifyDedicationComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) {}

//------------------------------------------------------------------------------
// createArchiveFile
//------------------------------------------------------------------------------
void SqliteCatalogue::createArchiveFile(
  const common::dataStructures::ArchiveFile &archiveFile) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ARCHIVE_FILE("
      "ARCHIVE_FILE_ID,"
      "DISK_INSTANCE,"
      "DISK_FILE_ID,"
      "FILE_SIZE,"
      "CHECKSUM_TYPE,"
      "CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME,"
      "CREATION_TIME,"
      "RECONCILIATION_TIME,"

      "RECOVERY_PATH,"
      "RECOVERY_OWNER,"
      "RECOVERY_GROUP,"
      "RECOVERY_BLOB)"

    "VALUES("
      ":ARCHIVE_FILE_ID,"
      ":DISK_INSTANCE,"
      ":DISK_FILE_ID,"
      ":FILE_SIZE,"
      ":CHECKSUM_TYPE,"
      ":CHECKSUM_VALUE,"
      ":STORAGE_CLASS_NAME,"
      ":CREATION_TIME,"
      ":RECONCILIATION_TIME,"

      ":RECOVERY_PATH,"
      ":RECOVERY_OWNER,"
      ":RECOVERY_GROUP,"
      ":RECOVERY_BLOB);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":ARCHIVE_FILE_ID", archiveFile.archiveFileID);
  stmt->bind(":DISK_INSTANCE", archiveFile.diskInstance);
  stmt->bind(":DISK_FILE_ID", archiveFile.diskFileID);
  stmt->bind(":FILE_SIZE", archiveFile.fileSize);
  stmt->bind(":CHECKSUM_TYPE", archiveFile.checksumType);
  stmt->bind(":CHECKSUM_VALUE", archiveFile.checksumValue);
  stmt->bind(":STORAGE_CLASS_NAME", archiveFile.storageClass);
  stmt->bind(":CREATION_TIME", now);
  stmt->bind(":RECONCILIATION_TIME", now);

  stmt->bind(":RECOVERY_PATH", archiveFile.drData.drPath);
  stmt->bind(":RECOVERY_OWNER", archiveFile.drData.drOwner);
  stmt->bind(":RECOVERY_GROUP",  archiveFile.drData.drGroup);
  stmt->bind(":RECOVERY_BLOB", archiveFile.drData.drBlob);

  stmt->step();
}

//------------------------------------------------------------------------------
// getArchiveFileId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getArchiveFileId(const std::string &diskInstance, const std::string &diskFileId) const {
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
    "FROM ARCHIVE_FILE WHERE "
      "DISK_INSTANCE = :DISK_INSTANCE AND "
      "DISK_FILE_ID = :DISK_FILE_ID;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":DISK_INSTANCE", diskInstance);
  stmt->bind(":DISK_FILE_ID", diskFileId);

  if(SQLITE_ROW != stmt->step()) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Could not find archive file from disk instance " << diskInstance <<
      " with disk file ID " << diskFileId;
    throw ex;
  } else {
    const ColumnNameToIdx nameToIdx = stmt->getColumnNameToIdx();
    return stmt->columnUint64(nameToIdx["ARCHIVE_FILE_ID"]);
  }
}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile>
  SqliteCatalogue::getArchiveFiles(
  const std::string &id,
  const std::string &eosid,
  const std::string &copynb,
  const std::string &tapepool,
  const std::string &vid,
  const std::string &owner,
  const std::string &group,
  const std::string &storageclass,
  const std::string &path) {
  std::list<cta::common::dataStructures::ArchiveFile> files;
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE_ID     AS ARCHIVE_FILE_ID,"
      "DISK_INSTANCE       AS DISK_INSTANCE,"
      "DISK_FILE_ID        AS DISK_FILE_ID,"
      "FILE_SIZE           AS FILE_SIZE,"
      "CHECKSUM_TYPE       AS CHECKSUM_TYPE,"
      "CHECKSUM_VALUE      AS CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME  AS STORAGE_CLASS_NAME,"
      "CREATION_TIME       AS CREATION_TIME,"
      "RECONCILIATION_TIME AS RECONCILIATION_TIME,"

      "RECOVERY_PATH       AS RECOVERY_PATH,"
      "RECOVERY_OWNER      AS RECOVERY_OWNER,"
      "RECOVERY_GROUP      AS RECOVERY_GROUP,"
      "RECOVERY_BLOB       AS RECOVERY_BLOB "

    "FROM ARCHIVE_FILE;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::ArchiveFile file;

    file.archiveFileID = stmt->columnUint64(nameToIdx["ARCHIVE_FILE_ID"]);
    file.diskFileID = stmt->columnText(nameToIdx["DISK_FILE_ID"]);
    file.fileSize = stmt->columnUint64(nameToIdx["FILE_SIZE"]);
    file.checksumType = stmt->columnText(nameToIdx["CHECKSUM_TYPE"]);
    file.checksumValue = stmt->columnText(nameToIdx["CHECKSUM_VALUE"]);
    file.storageClass = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);

    file.diskInstance = stmt->columnText(nameToIdx["DISK_INSTANCE"]);
    file.drData.drPath = stmt->columnText(nameToIdx["RECOVERY_PATH"]);
    file.drData.drOwner = stmt->columnText(nameToIdx["RECOVERY_OWNER"]);
    file.drData.drGroup = stmt->columnText(nameToIdx["RECOVERY_GROUP"]);
    file.drData.drBlob = stmt->columnText(nameToIdx["RECOVERY_BLOB"]);
    
    file.creationTime = stmt->columnUint64(nameToIdx["CREATION_TIME"]);
    file.reconciliationTime = stmt->columnUint64(nameToIdx["RECONCILIATION_TIME"]);

    files.push_back(file);
  }
  return files;
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary SqliteCatalogue::getArchiveFileSummary(const std::string &id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile SqliteCatalogue::
  getArchiveFileById(const uint64_t id) {
  const char *const sql =
    "SELECT "
      "ARCHIVE_FILE_ID     AS ARCHIVE_FILE_ID,"
      "DISK_INSTANCE       AS DISK_INSTANCE,"
      "DISK_FILE_ID        AS DISK_FILE_ID,"
      "FILE_SIZE           AS FILE_SIZE,"
      "CHECKSUM_TYPE       AS CHECKSUM_TYPE,"
      "CHECKSUM_VALUE      AS CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME  AS STORAGE_CLASS_NAME,"
      "CREATION_TIME       AS CREATION_TIME,"
      "RECONCILIATION_TIME AS RECONCILIATION_TIME,"

      "RECOVERY_PATH       AS RECOVERY_PATH,"
      "RECOVERY_OWNER      AS RECOVERY_OWNER,"
      "RECOVERY_GROUP      AS RECOVERY_GROUP,"
      "RECOVERY_BLOB       AS RECOVERY_BLOB "

    "FROM ARCHIVE_FILE WHERE "
      "ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":ARCHIVE_FILE_ID", id);

  if(SQLITE_ROW != stmt->step()) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Could not find archive file with ID " << id;
    throw ex;
  } else {
    const ColumnNameToIdx nameToIdx = stmt->getColumnNameToIdx();
    cta::common::dataStructures::ArchiveFile file;

    file.archiveFileID = id;
    file.diskFileID = stmt->columnText(nameToIdx["DISK_FILE_ID"]);
    file.fileSize = stmt->columnUint64(nameToIdx["FILE_SIZE"]);
    file.checksumType = stmt->columnText(nameToIdx["CHECKSUM_TYPE"]);
    file.checksumValue = stmt->columnText(nameToIdx["CHECKSUM_VALUE"]);
    file.storageClass = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);

    file.diskInstance = stmt->columnText(nameToIdx["DISK_INSTANCE"]);
    file.drData.drPath = stmt->columnText(nameToIdx["RECOVERY_PATH"]);
    file.drData.drOwner = stmt->columnText(nameToIdx["RECOVERY_OWNER"]);
    file.drData.drGroup = stmt->columnText(nameToIdx["RECOVERY_GROUP"]);
    file.drData.drBlob = stmt->columnText(nameToIdx["RECOVERY_BLOB"]);
    
    file.creationTime = stmt->columnUint64(nameToIdx["CREATION_TIME"]);
    file.reconciliationTime = stmt->columnUint64(nameToIdx["RECONCILIATION_TIME"]);

    return file;
  }
}
          
//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void SqliteCatalogue::setDriveStatus(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void SqliteCatalogue::fileWrittenToTape(
  const cta::common::dataStructures::ArchiveRequest &archiveRequest,
  const cta::common::dataStructures::TapeFile &tapeFileLocation) {
}

//------------------------------------------------------------------------------
// prepareForNewFile
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileQueueCriteria
  SqliteCatalogue::prepareForNewFile(
  const std::string &storageClass, const cta::common::dataStructures::UserIdentity &user) {
  const common::dataStructures::TapeCopyToPoolMap copyToPoolMap =
    getTapeCopyToPoolMap(storageClass);
  const uint64_t expectedNbRoutes = getExpectedNbArchiveRoutes(storageClass);

  // Check that the number of archive routes is correct
  if(copyToPoolMap.empty()) {
    exception::Exception ex;
    ex.getMessage() << "Storage class " << storageClass << " has no archive"
      " routes";
    throw ex;
  }
  if(copyToPoolMap.size() != expectedNbRoutes) {
    exception::Exception ex;
    ex.getMessage() << "Storage class " << storageClass << " does not have the"
      " expected number of archive routes routes: expected=" << expectedNbRoutes
      << ", actual=" << copyToPoolMap.size();
    throw ex;
  }

  common::dataStructures::MountPolicy mountPolicy = getMountPolicyForAUser(user);

  // Now that we have both the archive routes and the mount policy it's safe to
  // consume an archive file identifier
  const uint64_t archiveFileId = m_nextArchiveFileId++;

  return common::dataStructures::ArchiveFileQueueCriteria(archiveFileId,
    copyToPoolMap, mountPolicy);
}

//------------------------------------------------------------------------------
// getTapeCopyToPoolMap
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeCopyToPoolMap SqliteCatalogue::
  getTapeCopyToPoolMap(const std::string &storageClass) const {
  cta::common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
  const char *const sql =
    "SELECT "
      "COPY_NB        AS COPY_NB,"
      "TAPE_POOL_NAME AS TAPE_POOL_NAME "
    "FROM ARCHIVE_ROUTE WHERE "
      "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":STORAGE_CLASS_NAME", storageClass);
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if (nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    const uint64_t copyNb = stmt->columnUint64(nameToIdx["COPY_NB"]);
    const std::string tapePoolName = stmt->columnText(nameToIdx["TAPE_POOL_NAME"]);
    copyToPoolMap[copyNb] = tapePoolName;
  }

  return copyToPoolMap;
}

//------------------------------------------------------------------------------
// getExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getExpectedNbArchiveRoutes(const std::string &storageClass) const {
  uint64_t nbRoutes = 0;
  const char *const sql =
    "SELECT "
      "COUNT(*) AS NB_ROUTES "
    "FROM ARCHIVE_ROUTE WHERE "
      "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":STORAGE_CLASS_NAME", storageClass);
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if (nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    nbRoutes = stmt->columnUint64(nameToIdx["NB_ROUTES"]);
  }
  return nbRoutes;
}

//------------------------------------------------------------------------------
// getArchiveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy SqliteCatalogue::
  getMountPolicyForAUser(const cta::common::dataStructures::UserIdentity &user) const {
  const char *const sql =
    "SELECT "
      "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

      "ARCHIVE_PRIORITY        AS ARCHIVE_PRIORITY,"
      "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
      "RETRIEVE_PRIORITY        AS RETRIEVE_PRIORITY,"
      "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

      "MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

      "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"

      "MOUNT_POLICY.CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "MOUNT_POLICY.CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "MOUNT_POLICY.CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "MOUNT_POLICY.CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "MOUNT_POLICY.LAST_UPDATE_USER_NAME  AS LAST_UPDATE_USER_NAME,"
      "MOUNT_POLICY.LAST_UPDATE_GROUP_NAME AS LAST_UPDATE_GROUP_NAME,"
      "MOUNT_POLICY.LAST_UPDATE_HOST_NAME  AS LAST_UPDATE_HOST_NAME,"
      "MOUNT_POLICY.LAST_UPDATE_TIME       AS LAST_UPDATE_TIME "
    "FROM MOUNT_POLICY INNER JOIN REQUESTER ON "
      "MOUNT_POLICY.MOUNT_POLICY_NAME = REQUESTER.MOUNT_POLICY_NAME "
    "WHERE "
      "REQUESTER.REQUESTER_NAME = :REQUESTER_NAME;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":REQUESTER_NAME", user.name);
  ColumnNameToIdx nameToIdx;
  if(SQLITE_ROW == stmt->step()) {
    nameToIdx = stmt->getColumnNameToIdx();    
    common::dataStructures::MountPolicy policy;

    policy.name = stmt->columnText(nameToIdx["MOUNT_POLICY_NAME"]);

    policy.archive_priority = stmt->columnUint64(nameToIdx["ARCHIVE_PRIORITY"]);
    policy.archive_minRequestAge =
      stmt->columnUint64(nameToIdx["ARCHIVE_MIN_REQUEST_AGE"]);

    policy.retrieve_priority =
      stmt->columnUint64(nameToIdx["RETRIEVE_PRIORITY"]);
    policy.retrieve_minRequestAge =
      stmt->columnUint64(nameToIdx["RETRIEVE_MIN_REQUEST_AGE"]);

    policy.maxDrivesAllowed =
      stmt->columnUint64(nameToIdx["MAX_DRIVES_ALLOWED"]);

    policy.comment = stmt->columnText(nameToIdx["USER_COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    policy.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_UPDATE_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_UPDATE_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_UPDATE_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_UPDATE_TIME"]);

    policy.lastModificationLog = updateLog;
    return policy;
  } else {
    exception::Exception ex;
    ex.getMessage() << "Failed to find a mount policy for user " <<
      user;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool SqliteCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &cliIdentity) const {
  return userIsAdmin(cliIdentity.user.name) && hostIsAdmin(cliIdentity.host);
}

//------------------------------------------------------------------------------
// userIsAdmin
//------------------------------------------------------------------------------
bool SqliteCatalogue::userIsAdmin(const std::string &userName)
  const {
  const char *const sql =
    "SELECT "
      "ADMIN_USER_NAME AS ADMIN_USER_NAME "
    "FROM ADMIN_USER WHERE "
      "ADMIN_USER_NAME = :ADMIN_USER_NAME;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":ADMIN_USER_NAME", userName);
  if(SQLITE_ROW == stmt->step()) {
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// hostIsAdmin
//------------------------------------------------------------------------------
bool SqliteCatalogue::hostIsAdmin(const std::string &hostName)
  const {
  const char *const sql =
    "SELECT "
      "ADMIN_HOST_NAME AS ADMIN_HOST_NAME "
    "FROM ADMIN_HOST WHERE "
      "ADMIN_HOST_NAME = :ADMIN_HOST_NAME;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":ADMIN_HOST_NAME", hostName);
  if(SQLITE_ROW == stmt->step()) {
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// createTapeFile
//------------------------------------------------------------------------------
void SqliteCatalogue::createTapeFile(const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO TAPE_FILE("
      "VID,"
      "FSEQ,"
      "BLOCK_ID,"
      "CREATION_TIME,"
      "ARCHIVE_FILE_ID)"
    "VALUES("
      ":VID,"
      ":FSEQ,"
      ":BLOCK_ID,"
      ":CREATION_TIME,"
      ":ARCHIVE_FILE_ID);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":VID", tapeFile.vid);
  stmt->bind(":FSEQ", tapeFile.fSeq);
  stmt->bind(":BLOCK_ID", tapeFile.blockId);
  stmt->bind(":CREATION_TIME", now);
  stmt->bind(":ARCHIVE_FILE_ID", archiveFileId);

  stmt->step();
}

//------------------------------------------------------------------------------
// getTapeFiles
//------------------------------------------------------------------------------
std::list<common::dataStructures::TapeFile> SqliteCatalogue::getTapeFiles() const {
  std::list<cta::common::dataStructures::TapeFile> files;
  const char *const sql =
    "SELECT "
      "VID           AS VID,"
      "FSEQ          AS FSEQ,"
      "BLOCK_ID      AS BLOCK_ID,"
      "CREATION_TIME AS CREATION_TIME "
    "FROM TAPE_FILE;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::TapeFile file;

    file.vid = stmt->columnText(nameToIdx["VID"]);
    file.fSeq = stmt->columnUint64(nameToIdx["FSEQ"]);
    file.blockId = stmt->columnUint64(nameToIdx["BLOCK_ID"]);
    file.creationTime = stmt->columnUint64(nameToIdx["CREATION_TIME"]);

    files.push_back(file);
  }

  return files;
}

//------------------------------------------------------------------------------
// setTapeLastFseq
//------------------------------------------------------------------------------
void SqliteCatalogue::setTapeLastFSeq(const std::string &vid, const uint64_t lastFSeq) {
  std::lock_guard<std::mutex> lock(m_mutex);

  const uint64_t currentValue = getTapeLastFSeq(vid);
  if(lastFSeq != currentValue + 1) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: The last FSeq MUST be incremented by exactly one: "
      "currentValue=" << currentValue << ",nextValue=" << lastFSeq;
    throw ex;
  }
  const char *const sql =
    "UPDATE TAPE SET "
      "LAST_FSEQ = :LAST_FSEQ "
    "WHERE "
      "VID=:VID;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":VID", vid);
  stmt->bind(":LAST_FSEQ", lastFSeq);
  stmt->step();
}

//------------------------------------------------------------------------------
// getTapeLastFSeq
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getTapeLastFSeq(const std::string &vid) const {
  const char *const sql =
    "SELECT "
      "LAST_FSEQ AS LAST_FSEQ "
    "FROM TAPE WHERE "
      "VID = :VID;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  stmt->bind(":VID", vid);
  if(SQLITE_ROW != stmt->step()) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: No such tape with vid=" << vid;
    throw ex;
  }
  ColumnNameToIdx nameToIdx = stmt->getColumnNameToIdx();
  return stmt->columnUint64(nameToIdx["LAST_FSEQ"]);
}

} // namespace catalogue
} // namespace cta
