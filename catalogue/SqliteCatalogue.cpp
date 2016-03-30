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
#include "catalogue/SqliteStmt.hpp"
#include "common/exception/Exception.hpp"

#include <memory>
#include <sqlite3.h>
#include <time.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteCatalogue::SqliteCatalogue(): m_conn(":memory:") {
  createDbSchema();
}

//------------------------------------------------------------------------------
// createSchema
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createDbSchema() {
  const char *const sql = 
    "CREATE TABLE ADMIN_USER("
      "ADMIN_USER_NAME  TEXT,"
      "ADMIN_GROUP_NAME TEXT,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(ADMIN_USER_NAME)"
    ");"

    "CREATE TABLE ADMIN_HOST("
      "ADMIN_HOST_NAME TEXT,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(ADMIN_HOST_NAME)"
    ");"

    "CREATE TABLE STORAGE_CLASS("
      "STORAGE_CLASS_NAME TEXT,"
      "NB_COPIES          INTEGER,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(STORAGE_CLASS_NAME)"
    ");"

    "CREATE TABLE TAPE_POOL("
      "TAPE_POOL_NAME   TEXT,"
      "NB_PARTIAL_TAPES INTEGER,"
      "IS_ENCRYPTED     INTEGER,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(TAPE_POOL_NAME)"
    ");"

    "CREATE TABLE ARCHIVE_ROUTE("
      "STORAGE_CLASS_NAME TEXT,"
      "COPY_NB            INTEGER,"
      "TAPE_POOL_NAME     TEXT,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(STORAGE_CLASS_NAME, COPY_NB),"

      "FOREIGN KEY(STORAGE_CLASS_NAME) REFERENCES "
        "STORAGE_CLASS(STORAGE_CLASS_NAME),"
      "FOREIGN KEY(TAPE_POOL_NAME) REFERENCES "
        "TAPE_POOL(TAPE_POOL_NAME)"
    ");"

    "CREATE TABLE LOGICAL_LIBRARY("
      "LOGICAL_LIBRARY_NAME TEXT,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(LOGICAL_LIBRARY_NAME)"
    ");"

    "CREATE TABLE TAPE("
      "VID                  TEXT,"
      "LOGICAL_LIBRARY_NAME TEXT,"
      "TAPE_POOL_NAME       TEXT,"
      "ENCRYPTION_KEY       TEXT,"
      "CAPACITY_IN_BYTES    INTEGER,"
      "DATA_IN_BYTES        INTEGER,"
      "LAST_FSEQ            INTEGER,"
      "IS_BUSY              INTEGER,"
      "IS_DISABLED          INTEGER,"
      "IS_FULL              INTEGER,"
      "LBP_IS_ON            INTEGER,"

      "LABEL_DRIVE TEXT,"
      "LABEL_TIME  INTEGER,"

      "LAST_READ_DRIVE TEXT,"
      "LAST_READ_TIME  INTEGER,"

      "LAST_WRITE_DRIVE TEXT,"
      "LAST_WRITE_TIME  INTEGER,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(VID),"

      "FOREIGN KEY(LOGICAL_LIBRARY_NAME) REFERENCES "
        "LOGICAL_LIBRARY(LOGICAL_LIBRARY_NAME),"
      "FOREIGN KEY(TAPE_POOL_NAME) REFERENCES "
        "TAPE_POOL(TAPE_POOL_NAME)"
    ");"
    "CREATE TABLE MOUNT_GROUP("
      "MOUNT_GROUP_NAME TEXT,"

      "ARCHIVE_PRIORITY         INTEGER,"
      "MIN_ARCHIVE_FILES_QUEUED INTEGER,"
      "MIN_ARCHIVE_BYTES_QUEUED INTEGER,"
      "MIN_ARCHIVE_REQUEST_AGE  INTEGER,"

      "RETRIEVE_PRIORITY         INTEGER,"
      "MIN_RETRIEVE_FILES_QUEUED INTEGER,"
      "MIN_RETRIEVE_BYTES_QUEUED INTEGER,"
      "MIN_RETRIEVE_REQUEST_AGE  INTEGER,"

      "MAX_DRIVES_ALLOWED INTEGER,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(MOUNT_GROUP_NAME)"
    ");"
    "CREATE TABLE USER("
      "USER_NAME  TEXT,"
      "GROUP_NAME TEXT,"
      "MOUNT_GROUP_NAME TEXT,"

      "COMMENT TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(USER_NAME),"
      "FOREIGN KEY(MOUNT_GROUP_NAME) REFERENCES "
        "MOUNT_GROUP(MOUNT_GROUP_NAME)"
    ");"
    "CREATE TABLE ARCHIVE_FILE("
      "ARCHIVE_FILE_ID    INTEGER AUTO INCREMENT,"
      "DISK_INSTANCE      STRING,"
      "DISK_FILE_ID       STRING,"
      "FILE_SIZE          INTEGER,"
      "CHECKSUM_TYPE      STRING,"
      "CHECKSUM_VALUE     STRING,"
      "STORAGE_CLASS_NAME STRING,"
      "CREATION_TIME      INTEGER,"

      "RECOVERY_PATH     STRING,"
      "RECOVERY_OWNER    STRING,"
      "RECOVERY_GROUP    STRING,"
      "RECOVERY_BLOB     STRING,"

      "PRIMARY KEY(ARCHIVE_FILE_ID),"
      "FOREIGN KEY(STORAGE_CLASS_NAME) "
        "REFERENCES STORAGE_CLASS(STORAGE_CLASS_NAME),"
      "UNIQUE(DISK_INSTANCE, DISK_FILE_ID)"
    ");"
    "CREATE TABLE TAPE_FILE("
      "VID             STRING,"
      "FSEQ            INTEGER,"
      "BLOCK_ID        INTEGER,"
      "CREATION_TIME   INTEGER,"
      "ARCHIVE_FILE_ID INTEGER,"

      "PRIMARY KEY(VID, FSEQ),"
      "FOREIGN KEY(ARCHIVE_FILE_ID) "
        "REFERENCES ARCHIVE_FILE(ARCHIVE_FILE_ID),"
      "UNIQUE(FSEQ, BLOCK_ID)"
    ");";
  m_conn.enableForeignKeys();
  m_conn.execNonQuery(sql);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteCatalogue::~SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createBootstrapAdminAndHostNoAuth(
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
void cta::catalogue::SqliteCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const common::dataStructures::UserIdentity &user,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ADMIN_USER("
      "ADMIN_USER_NAME,"
      "ADMIN_GROUP_NAME,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":ADMIN_USER_NAME,"
      ":ADMIN_GROUP_NAME,"

      ":COMMENT,"

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
  stmt->bind(":ADMIN_GROUP_NAME", user.group);

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteAdminUser(const common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser>
  cta::catalogue::SqliteCatalogue::getAdminUsers() const {
  std::list<common::dataStructures::AdminUser> admins;
  const char *const sql =
    "SELECT "
      "ADMIN_USER_NAME  AS ADMIN_USER_NAME,"
      "ADMIN_GROUP_NAME AS ADMIN_GROUP_NAME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM ADMIN_USER;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::AdminUser admin;

    common::dataStructures::UserIdentity adminUI;
    adminUI.name = stmt->columnText(nameToIdx["ADMIN_USER_NAME"]);
    adminUI.group = stmt->columnText(nameToIdx["ADMIN_GROUP_NAME"]);

    admin.user = adminUI;

    admin.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    admin.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    admin.lastModificationLog = updateLog;

    admins.push_back(admin);
  }

  return admins;
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createAdminHost(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &hostName,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ADMIN_HOST("
      "ADMIN_HOST_NAME,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":ADMIN_HOST_NAME,"

      ":COMMENT,"

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

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteAdminHost(const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::catalogue::SqliteCatalogue::getAdminHosts() const {
  std::list<common::dataStructures::AdminHost> hosts;
  const char *const sql =
    "SELECT "
      "ADMIN_HOST_NAME AS ADMIN_HOST_NAME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM ADMIN_HOST;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::AdminHost host;

    host.name = stmt->columnText(nameToIdx["ADMIN_HOST_NAME"]);
    host.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    host.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    host.lastModificationLog = updateLog;

    hosts.push_back(host);
  }

  return hosts;
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyAdminHostComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t nbCopies,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO STORAGE_CLASS("
      "STORAGE_CLASS_NAME,"
      "NB_COPIES,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":STORAGE_CLASS_NAME,"
      ":NB_COPIES,"

      ":COMMENT,"

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

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteStorageClass(const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass>
  cta::catalogue::SqliteCatalogue::getStorageClasses() const {
  std::list<common::dataStructures::StorageClass> storageClasses;
  const char *const sql =
    "SELECT "
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "NB_COPIES          AS NB_COPIES,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
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
    storageClass.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    storageClass.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    storageClass.lastModificationLog = updateLog;

    storageClasses.push_back(storageClass);
  }

  return storageClasses;
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) {}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createTapePool(
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

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":TAPE_POOL_NAME,"
      ":NB_PARTIAL_TAPES,"
      ":IS_ENCRYPTED,"

      ":COMMENT,"

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

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteTapePool(const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool>
  cta::catalogue::SqliteCatalogue::getTapePools() const {
  std::list<cta::common::dataStructures::TapePool> pools;
  const char *const sql =
    "SELECT "
      "TAPE_POOL_NAME   AS TAPE_POOL_NAME,"
      "NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
      "IS_ENCRYPTED     AS IS_ENCRYPTED,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
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

    pool.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    pool.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    pool.lastModificationLog = updateLog;

    pools.push_back(pool);
  }

  return pools;
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) {}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setTapePoolEncryption(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) {}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createArchiveRoute(
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

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":STORAGE_CLASS_NAME,"
      ":COPY_NB,"
      ":TAPE_POOL_NAME,"

      ":COMMENT,"

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

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute>
  cta::catalogue::SqliteCatalogue::getArchiveRoutes() const {
  std::list<common::dataStructures::ArchiveRoute> routes;
  const char *const sql =
    "SELECT "
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "COPY_NB            AS COPY_NB,"
      "TAPE_POOL_NAME     AS TAPE_POOL_NAME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
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

    route.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    route.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    route.lastModificationLog = updateLog;

    routes.push_back(route);
  }

  return routes;
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO LOGICAL_LIBRARY("
      "LOGICAL_LIBRARY_NAME,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":LOGICAL_LIBRARY_NAME,"

      ":COMMENT,"

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

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteLogicalLibrary(const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary>
  cta::catalogue::SqliteCatalogue::getLogicalLibraries() const {
  std::list<cta::common::dataStructures::LogicalLibrary> libs;
  const char *const sql =
    "SELECT "
      "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM LOGICAL_LIBRARY;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::LogicalLibrary lib;

    lib.name = stmt->columnText(nameToIdx["LOGICAL_LIBRARY_NAME"]);

    lib.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    lib.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    lib.lastModificationLog = updateLog;

    libs.push_back(lib);
  }

  return libs;
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createTape(
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
      "IS_BUSY,"
      "IS_DISABLED,"
      "IS_FULL,"
      "LBP_IS_ON,"

      "LABEL_DRIVE,"
      "LABEL_TIME,"

      "LAST_READ_DRIVE,"
      "LAST_READ_TIME,"

      "LAST_WRITE_DRIVE,"
      "LAST_WRITE_TIME,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":VID,"
      ":LOGICAL_LIBRARY_NAME,"
      ":TAPE_POOL_NAME,"
      ":ENCRYPTION_KEY,"
      ":CAPACITY_IN_BYTES,"
      ":DATA_IN_BYTES,"
      ":LAST_FSEQ,"
      ":IS_BUSY,"
      ":IS_DISABLED,"
      ":IS_FULL,"
      ":LBP_IS_ON,"

      ":LABEL_DRIVE,"
      ":LABEL_TIME,"

      ":LAST_READ_DRIVE,"
      ":LAST_READ_TIME,"

      ":LAST_WRITE_DRIVE,"
      ":LAST_WRITE_TIME,"

      ":COMMENT,"

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
  stmt->bind(":IS_BUSY", 0);
  stmt->bind(":IS_DISABLED", disabledValue);
  stmt->bind(":IS_FULL", fullValue);
  stmt->bind(":LBP_IS_ON", 1);

  stmt->bind(":LABEL_DRIVE", "");
  stmt->bind(":LABEL_TIME", 0);

  stmt->bind(":LAST_READ_DRIVE", "");
  stmt->bind(":LAST_READ_TIME", 0);

  stmt->bind(":LAST_WRITE_DRIVE", "");
  stmt->bind(":LAST_WRITE_TIME", 0);

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteTape(const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape>
  cta::catalogue::SqliteCatalogue::getTapes(
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
      "IS_BUSY              AS IS_BUSY,"
      "IS_DISABLED          AS IS_DISABLED,"
      "IS_FULL              AS IS_FULL,"
      "LBP_IS_ON            AS LBP_IS_ON,"

      "LABEL_DRIVE AS LABEL_DRIVE,"
      "LABEL_TIME  AS LABEL_TIME,"

      "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "LAST_READ_TIME  AS LAST_READ_TIME,"

      "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "LAST_WRITE_TIME  AS LAST_WRITE_TIME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
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
    tape.busy = stmt->columnUint64(nameToIdx["IS_BUSY"]);
    tape.disabled = stmt->columnUint64(nameToIdx["IS_DISABLED"]);
    tape.full = stmt->columnUint64(nameToIdx["IS_FULL"]);
    tape.lbp = stmt->columnUint64(nameToIdx["LBP_IS_ON"]);

    tape.labelLog.drive = stmt->columnText(nameToIdx["LABEL_DRIVE"]);
    tape.labelLog.time = stmt->columnUint64(nameToIdx["LABEL_TIME"]);

    tape.lastReadLog.drive = stmt->columnText(nameToIdx["LAST_READ_DRIVE"]);
    tape.lastReadLog.time = stmt->columnUint64(nameToIdx["LAST_READ_TIME"]);

    tape.lastWriteLog.drive = stmt->columnText(nameToIdx["LAST_WRITE_DRIVE"]);
    tape.lastWriteLog.time = stmt->columnUint64(nameToIdx["LAST_WRITE_TIME"]);

    tape.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    tape.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    tape.lastModificationLog = updateLog;

    tapes.push_back(tape);
  }

  return tapes;
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) {}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) {}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeEncryptionKey(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) {}

//------------------------------------------------------------------------------
// modifyTapeLabelLog
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeLabelLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastWrittenLog
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeLastWrittenLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastReadLog
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeLastReadLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setTapeBusy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) {}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) {}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) {}

//------------------------------------------------------------------------------
// setTapeLbp
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) {}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createUser(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const std::string &group,
  const std::string &mountGroup,
  const std::string &comment) {
  const uint64_t now = time(NULL);
  const char *const sql =
    "INSERT INTO USER("
      "USER_NAME,"
      "GROUP_NAME,"
      "MOUNT_GROUP_NAME,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":USER_NAME,"
      ":GROUP_NAME,"
      ":MOUNT_GROUP_NAME,"

      ":COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":USER_NAME", name);
  stmt->bind(":GROUP_NAME", group);
  stmt->bind(":MOUNT_GROUP_NAME", mountGroup);

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteUser(const std::string &name, const std::string &group) {}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User>
  cta::catalogue::SqliteCatalogue::getUsers() const {
  std::list<common::dataStructures::User> users;
  const char *const sql =
    "SELECT "
      "USER_NAME        AS USER_NAME,"
      "GROUP_NAME       AS GROUP_NAME,"
      "MOUNT_GROUP_NAME AS MOUNT_GROUP_NAME,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM USER;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::User user;

    common::dataStructures::UserIdentity adminUI;
    user.name = stmt->columnText(nameToIdx["USER_NAME"]);
    user.group = stmt->columnText(nameToIdx["GROUP_NAME"]);
    user.mountGroupName = stmt->columnText(nameToIdx["MOUNT_GROUP_NAME"]);

    user.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    user.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    user.lastModificationLog = updateLog;

    users.push_back(user);
  }

  return users;
}

//------------------------------------------------------------------------------
// modifyUserMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyUserMountGroup(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup) {}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyUserComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &comment) {}

//------------------------------------------------------------------------------
// createMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createMountGroup(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &name,
  const uint64_t archivePriority,
  const uint64_t minArchiveFilesQueued, 
  const uint64_t minArchiveBytesQueued,
  const uint64_t minArchiveRequestAge,
  const uint64_t retrievePriority,
  const uint64_t minRetrieveFilesQueued,
  const uint64_t minRetrieveBytesQueued,
  const uint64_t minRetrieveRequestAge,
  const uint64_t maxDrivesAllowed,
  const std::string &comment) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO MOUNT_GROUP("
      "MOUNT_GROUP_NAME,"

      "ARCHIVE_PRIORITY,"
      "MIN_ARCHIVE_FILES_QUEUED,"
      "MIN_ARCHIVE_BYTES_QUEUED,"
      "MIN_ARCHIVE_REQUEST_AGE,"

      "RETRIEVE_PRIORITY,"
      "MIN_RETRIEVE_FILES_QUEUED,"
      "MIN_RETRIEVE_BYTES_QUEUED,"
      "MIN_RETRIEVE_REQUEST_AGE,"

      "MAX_DRIVES_ALLOWED,"

      "COMMENT,"

      "CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME)"
    "VALUES("
      ":MOUNT_GROUP_NAME,"

      ":ARCHIVE_PRIORITY,"
      ":MIN_ARCHIVE_FILES_QUEUED,"
      ":MIN_ARCHIVE_BYTES_QUEUED,"
      ":MIN_ARCHIVE_REQUEST_AGE,"

      ":RETRIEVE_PRIORITY,"
      ":MIN_RETRIEVE_FILES_QUEUED,"
      ":MIN_RETRIEVE_BYTES_QUEUED,"
      ":MIN_RETRIEVE_REQUEST_AGE,"

      ":MAX_DRIVES_ALLOWED,"

      ":COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":MOUNT_GROUP_NAME", name);

  stmt->bind(":ARCHIVE_PRIORITY", archivePriority);
  stmt->bind(":MIN_ARCHIVE_FILES_QUEUED", minArchiveFilesQueued);
  stmt->bind(":MIN_ARCHIVE_BYTES_QUEUED", minArchiveBytesQueued);
  stmt->bind(":MIN_ARCHIVE_REQUEST_AGE", minArchiveRequestAge);

  stmt->bind(":RETRIEVE_PRIORITY", retrievePriority);
  stmt->bind(":MIN_RETRIEVE_FILES_QUEUED", minRetrieveFilesQueued);
  stmt->bind(":MIN_RETRIEVE_BYTES_QUEUED", minRetrieveBytesQueued);
  stmt->bind(":MIN_RETRIEVE_REQUEST_AGE", minRetrieveRequestAge);

  stmt->bind(":MAX_DRIVES_ALLOWED", maxDrivesAllowed);

  stmt->bind(":COMMENT", comment);

  stmt->bind(":CREATION_LOG_USER_NAME", cliIdentity.user.name);
  stmt->bind(":CREATION_LOG_GROUP_NAME", cliIdentity.user.group);
  stmt->bind(":CREATION_LOG_HOST_NAME", cliIdentity.host);
  stmt->bind(":CREATION_LOG_TIME", now);

  stmt->step();
}

//------------------------------------------------------------------------------
// deleteMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteMountGroup(const std::string &name) {}

//------------------------------------------------------------------------------
// getMountGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::MountGroup>
  cta::catalogue::SqliteCatalogue::getMountGroups() const {
  std::list<cta::common::dataStructures::MountGroup> groups;
  const char *const sql =
    "SELECT "
      "MOUNT_GROUP_NAME AS MOUNT_GROUP_NAME,"

      "ARCHIVE_PRIORITY         AS ARCHIVE_PRIORITY,"
      "MIN_ARCHIVE_FILES_QUEUED AS MIN_ARCHIVE_FILES_QUEUED,"
      "MIN_ARCHIVE_BYTES_QUEUED AS MIN_ARCHIVE_BYTES_QUEUED,"
      "MIN_ARCHIVE_REQUEST_AGE  AS MIN_ARCHIVE_REQUEST_AGE,"

      "RETRIEVE_PRIORITY         AS RETRIEVE_PRIORITY,"
      "MIN_RETRIEVE_FILES_QUEUED AS MIN_RETRIEVE_FILES_QUEUED,"
      "MIN_RETRIEVE_BYTES_QUEUED AS MIN_RETRIEVE_BYTES_QUEUED,"
      "MIN_RETRIEVE_REQUEST_AGE  AS MIN_RETRIEVE_REQUEST_AGE,"

      "MAX_DRIVES_ALLOWED AS MAX_DRIVES_ALLOWED,"

      "COMMENT AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM MOUNT_GROUP;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::MountGroup group;

    group.name = stmt->columnText(nameToIdx["MOUNT_GROUP_NAME"]);

    group.archive_priority = stmt->columnUint64(nameToIdx["ARCHIVE_PRIORITY"]);
    group.archive_minFilesQueued =
      stmt->columnUint64(nameToIdx["MIN_ARCHIVE_FILES_QUEUED"]);
    group.archive_minBytesQueued =
      stmt->columnUint64(nameToIdx["MIN_ARCHIVE_BYTES_QUEUED"]);
    group.archive_minRequestAge =
      stmt->columnUint64(nameToIdx["MIN_ARCHIVE_REQUEST_AGE"]);

    group.retrieve_priority =
      stmt->columnUint64(nameToIdx["RETRIEVE_PRIORITY"]);
    group.retrieve_minFilesQueued =
      stmt->columnUint64(nameToIdx["MIN_RETRIEVE_FILES_QUEUED"]);
    group.retrieve_minBytesQueued =
      stmt->columnUint64(nameToIdx["MIN_RETRIEVE_BYTES_QUEUED"]);
    group.retrieve_minRequestAge =
      stmt->columnUint64(nameToIdx["MIN_RETRIEVE_REQUEST_AGE"]);

    group.maxDrivesAllowed =
      stmt->columnUint64(nameToIdx["MAX_DRIVES_ALLOWED"]);

    group.comment = stmt->columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt->columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt->columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt->columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt->columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    group.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt->columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt->columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt->columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt->columnUint64(nameToIdx["LAST_MOD_TIME"]);

    group.lastModificationLog = updateLog;

    groups.push_back(group);
  }

  return groups;
}

//------------------------------------------------------------------------------
// modifyMountGroupArchivePriority
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupArchivePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupArchiveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupArchiveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupRetrievePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupRetrieveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupRetrieveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) {}

//------------------------------------------------------------------------------
// modifyMountGroupComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyMountGroupComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &mountGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteDedication(const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::catalogue::SqliteCatalogue::getDedications() const { return std::list<cta::common::dataStructures::Dedication>();}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationType(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {}

//------------------------------------------------------------------------------
// modifyDedicationMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationMountGroup(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &mountGroup) {}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationTag(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) {}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationVid(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationFrom(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationUntil(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyDedicationComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) {}

//------------------------------------------------------------------------------
// createArchiveFile
//------------------------------------------------------------------------------
uint64_t cta::catalogue::SqliteCatalogue::createArchiveFile(
  const common::dataStructures::ArchiveFile &archiveFile) {
  const time_t now = time(NULL);
  const char *const sql =
    "INSERT INTO ARCHIVE_FILE("
      "DISK_INSTANCE,"
      "DISK_FILE_ID,"
      "FILE_SIZE,"
      "CHECKSUM_TYPE,"
      "CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME,"
      "CREATION_TIME,"

      "RECOVERY_PATH,"
      "RECOVERY_OWNER,"
      "RECOVERY_GROUP,"
      "RECOVERY_BLOB)"

    "VALUES("
      ":DISK_INSTANCE,"
      ":DISK_FILE_ID,"
      ":FILE_SIZE,"
      ":CHECKSUM_TYPE,"
      ":CHECKSUM_VALUE,"
      ":STORAGE_CLASS_NAME,"
      ":CREATION_TIME,"

      ":RECOVERY_PATH,"
      ":RECOVERY_OWNER,"
      ":RECOVERY_GROUP,"
      ":RECOVERY_BLOB);";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));

  stmt->bind(":DISK_INSTANCE", archiveFile.drData.drInstance);
  stmt->bind(":DISK_FILE_ID", archiveFile.eosFileID);
  stmt->bind(":FILE_SIZE", archiveFile.fileSize);
  stmt->bind(":CHECKSUM_TYPE", archiveFile.checksumType);
  stmt->bind(":CHECKSUM_VALUE", archiveFile.checksumValue);
  stmt->bind(":STORAGE_CLASS_NAME", archiveFile.storageClass);
  stmt->bind(":CREATION_TIME", now);

  stmt->bind(":RECOVERY_PATH", archiveFile.drData.drPath);
  stmt->bind(":RECOVERY_OWNER", archiveFile.drData.drOwner);
  stmt->bind(":RECOVERY_GROUP",  archiveFile.drData.drGroup);
  stmt->bind(":RECOVERY_BLOB", archiveFile.drData.drBlob);

  stmt->step();

  return getArchiveFileId(archiveFile.drData.drInstance, archiveFile.eosFileID);
}

//------------------------------------------------------------------------------
// getArchiveFileId
//------------------------------------------------------------------------------
uint64_t cta::catalogue::SqliteCatalogue::getArchiveFileId(
  const std::string &diskInstance, const std::string &diskFileId) const {
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
  cta::catalogue::SqliteCatalogue::getArchiveFiles(
  const uint64_t id,
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
      "DISK_INSTANCE      AS DISK_INSTANCE,"
      "DISK_FILE_ID       AS DISK_FILE_ID,"
      "FILE_SIZE          AS FILE_SIZE,"
      "CHECKSUM_TYPE      AS CHECKSUM_TYPE,"
      "CHECKSUM_VALUE     AS CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "CREATION_TIME      AS CREATION_TIME,"

      "RECOVERY_PATH      AS RECOVERY_PATH,"
      "RECOVERY_OWNER     AS RECOVERY_OWNER,"
      "RECOVERY_GROUP     AS RECOVERY_GROUP,"
      "RECOVERY_BLOB      AS RECOVERY_BLOB "

    "FROM ARCHIVE_FILE;";
  std::unique_ptr<SqliteStmt> stmt(m_conn.createStmt(sql));
  ColumnNameToIdx nameToIdx;
  while(SQLITE_ROW == stmt->step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt->getColumnNameToIdx();
    }
    common::dataStructures::ArchiveFile file;

    file.archiveFileID = id;
    file.eosFileID = stmt->columnText(nameToIdx["DISK_FILE_ID"]);
    file.fileSize = stmt->columnUint64(nameToIdx["FILE_SIZE"]);
    file.checksumType = stmt->columnText(nameToIdx["CHECKSUM_TYPE"]);
    file.checksumValue = stmt->columnText(nameToIdx["CHECKSUM_VALUE"]);
    file.storageClass = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);

    file.drData.drInstance = stmt->columnText(nameToIdx["DISK_INSTANCE"]);
    file.drData.drPath = stmt->columnText(nameToIdx["RECOVERY_PATH"]);
    file.drData.drOwner = stmt->columnText(nameToIdx["RECOVERY_OWNER"]);
    file.drData.drGroup = stmt->columnText(nameToIdx["RECOVERY_GROUP"]);
    file.drData.drBlob = stmt->columnText(nameToIdx["RECOVERY_BLOB"]);

    files.push_back(file);
  }
  return files;
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::catalogue::SqliteCatalogue::getArchiveFileSummary(const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile cta::catalogue::SqliteCatalogue::
  getArchiveFileById(const uint64_t id) {
  const char *const sql =
    "SELECT "
      "DISK_INSTANCE      AS DISK_INSTANCE,"
      "DISK_FILE_ID       AS DISK_FILE_ID,"
      "FILE_SIZE          AS FILE_SIZE,"
      "CHECKSUM_TYPE      AS CHECKSUM_TYPE,"
      "CHECKSUM_VALUE     AS CHECKSUM_VALUE,"
      "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
      "CREATION_TIME      AS CREATION_TIME,"

      "RECOVERY_PATH      AS RECOVERY_PATH,"
      "RECOVERY_OWNER     AS RECOVERY_OWNER,"
      "RECOVERY_GROUP     AS RECOVERY_GROUP,"
      "RECOVERY_BLOB      AS RECOVERY_BLOB "

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
    file.eosFileID = stmt->columnText(nameToIdx["DISK_FILE_ID"]);
    file.fileSize = stmt->columnUint64(nameToIdx["FILE_SIZE"]);
    file.checksumType = stmt->columnText(nameToIdx["CHECKSUM_TYPE"]);
    file.checksumValue = stmt->columnText(nameToIdx["CHECKSUM_VALUE"]);
    file.storageClass = stmt->columnText(nameToIdx["STORAGE_CLASS_NAME"]);

    file.drData.drInstance = stmt->columnText(nameToIdx["DISK_INSTANCE"]);
    file.drData.drPath = stmt->columnText(nameToIdx["RECOVERY_PATH"]);
    file.drData.drOwner = stmt->columnText(nameToIdx["RECOVERY_OWNER"]);
    file.drData.drGroup = stmt->columnText(nameToIdx["RECOVERY_GROUP"]);
    file.drData.drBlob = stmt->columnText(nameToIdx["RECOVERY_BLOB"]);

    return file;
  }
}
          
//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::setDriveStatus(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t cta::catalogue::SqliteCatalogue::getNextArchiveFileId() {
  return 0;
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::fileWrittenToTape(
  const cta::common::dataStructures::ArchiveRequest &archiveRequest,
  const cta::common::dataStructures::TapeFileLocation &tapeFileLocation) {
}

//------------------------------------------------------------------------------
// prepareForNewFile
//------------------------------------------------------------------------------
std::tuple<uint64_t, cta::common::dataStructures::TapeCopyToPoolMap,
  cta::common::dataStructures::MountPolicy>
  cta::catalogue::SqliteCatalogue::prepareForNewFile(
  const std::string &storageClass, const std::string &user) {
  const uint64_t fileId = 0;
  const common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
  const common::dataStructures::MountPolicy mountPolicy;
  return std::make_tuple(fileId, copyToPoolMap, mountPolicy);
}

//------------------------------------------------------------------------------
// getCopyNbToTapePoolMap
//------------------------------------------------------------------------------
std::map<uint64_t,std::string> cta::catalogue::SqliteCatalogue::
  getCopyNbToTapePoolMap(const std::string &storageClass) const {
  return std::map<uint64_t,std::string>();
}

//------------------------------------------------------------------------------
// getArchiveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::SqliteCatalogue::
  getArchiveMountPolicy(const common::dataStructures::UserIdentity &requester)
  const {
  return common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// getRetrieveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::SqliteCatalogue::
  getRetrieveMountPolicy(const common::dataStructures::UserIdentity &requester)
  const {
  return common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool cta::catalogue::SqliteCatalogue::isAdmin(
  const common::dataStructures::SecurityIdentity &cliIdentity) const {
  return userIsAdmin(cliIdentity.user.name) && hostIsAdmin(cliIdentity.host);
}

//------------------------------------------------------------------------------
// userIsAdmin
//------------------------------------------------------------------------------
bool cta::catalogue::SqliteCatalogue::userIsAdmin(const std::string &userName)
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
bool cta::catalogue::SqliteCatalogue::hostIsAdmin(const std::string &hostName)
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
