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
      "USER_NAME     TEXT,"
      "GROUP_NAME    TEXT,"
      "COMMENT       TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(USER_NAME)"
    ");"

    "CREATE TABLE ADMIN_HOST("
      "HOST_NAME     TEXT,"
      "COMMENT       TEXT,"

      "CREATION_LOG_USER_NAME  TEXT,"
      "CREATION_LOG_GROUP_NAME TEXT,"
      "CREATION_LOG_HOST_NAME  TEXT,"
      "CREATION_LOG_TIME       INTEGER,"

      "LAST_MOD_USER_NAME  TEXT,"
      "LAST_MOD_GROUP_NAME TEXT,"
      "LAST_MOD_HOST_NAME  TEXT,"
      "LAST_MOD_TIME       INTEGER,"

      "PRIMARY KEY(HOST_NAME)"
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
  const uint64_t now = time(NULL);

  common::dataStructures::EntryLog creationLog;
  creationLog.user.name = cliIdentity.user.name;
  creationLog.user.group = cliIdentity.user.group;
  creationLog.host = cliIdentity.host;
  creationLog.time = now;

  insertAdminUser(user, comment, creationLog);
  insertAdminHost(hostName, comment, creationLog);
}

//------------------------------------------------------------------------------
// insertAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::insertAdminUser(
  const common::dataStructures::UserIdentity &user,
  const std::string &comment,
  const common::dataStructures::EntryLog &creationLog) {
  const char *const sql =
    "INSERT INTO ADMIN_USER("
      "USER_NAME,"
      "GROUP_NAME,"
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
      ":COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  SqliteStmt stmt(m_conn, sql);

  stmt.bind(":USER_NAME", user.name);
  stmt.bind(":GROUP_NAME", user.group);
  stmt.bind(":COMMENT", comment);

  stmt.bind(":CREATION_LOG_USER_NAME", creationLog.user.name);
  stmt.bind(":CREATION_LOG_GROUP_NAME", creationLog.user.group);
  stmt.bind(":CREATION_LOG_HOST_NAME", creationLog.host);
  stmt.bind(":CREATION_LOG_TIME", creationLog.time);

  stmt.step();
}

//------------------------------------------------------------------------------
// insertAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::insertAdminHost(
  const std::string &hostName,
  const std::string &comment,
  const common::dataStructures::EntryLog &creationLog) {
  const char *const sql =
    "INSERT INTO ADMIN_HOST("
      "HOST_NAME,"
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
      ":HOST_NAME,"
      ":COMMENT,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME,"

      ":CREATION_LOG_USER_NAME,"
      ":CREATION_LOG_GROUP_NAME,"
      ":CREATION_LOG_HOST_NAME,"
      ":CREATION_LOG_TIME);";
  SqliteStmt stmt(m_conn, sql);

  stmt.bind(":HOST_NAME", hostName);
  stmt.bind(":COMMENT", comment);

  stmt.bind(":CREATION_LOG_USER_NAME", creationLog.user.name);
  stmt.bind(":CREATION_LOG_GROUP_NAME", creationLog.user.group);
  stmt.bind(":CREATION_LOG_HOST_NAME", creationLog.host);
  stmt.bind(":CREATION_LOG_TIME", creationLog.time);

  stmt.step();
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &cliIdentity,
  const common::dataStructures::UserIdentity &user,
  const std::string &comment) {
  const uint64_t now = time(NULL);

  common::dataStructures::EntryLog creationLog;
  creationLog.user.name = cliIdentity.user.name;
  creationLog.user.group = cliIdentity.user.group;
  creationLog.host = cliIdentity.host;
  creationLog.time = now;

  insertAdminUser(user, comment, creationLog);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteAdminUser(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser>
  cta::catalogue::SqliteCatalogue::getAdminUsers(
  const common::dataStructures::SecurityIdentity &requester) const {
  std::list<common::dataStructures::AdminUser> admins;
  const char *const sql =
    "SELECT "
      "USER_NAME  AS USER_NAME,"
      "GROUP_NAME AS GROUP_NAME,"
      "COMMENT    AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM ADMIN_USER";
  SqliteStmt stmt(m_conn, sql);
  ColumnNameToIdx  nameToIdx;
  while(SQLITE_ROW == stmt.step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt.getColumnNameToIdx();
    }
    common::dataStructures::AdminUser admin;

    common::dataStructures::UserIdentity adminUI;
    adminUI.name = stmt.columnText(nameToIdx["USER_NAME"]);
    adminUI.group = stmt.columnText(nameToIdx["GROUP_NAME"]);

    admin.user = adminUI;

    admin.comment = stmt.columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt.columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt.columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt.columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt.columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    admin.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt.columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt.columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt.columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt.columnUint64(nameToIdx["LAST_MOD_TIME"]);

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

  common::dataStructures::EntryLog creationLog;
  creationLog.user.name = cliIdentity.user.name;
  creationLog.user.group = cliIdentity.user.group;
  creationLog.host = cliIdentity.host;
  creationLog.time = now;

  insertAdminHost(hostName, comment, creationLog);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteAdminHost(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::catalogue::SqliteCatalogue::getAdminHosts(const common::dataStructures::SecurityIdentity &requester) const {
  std::list<common::dataStructures::AdminHost> hosts;
  const char *const sql =
    "SELECT "
      "HOST_NAME AS HOST_NAME,"
      "COMMENT   AS COMMENT,"

      "CREATION_LOG_USER_NAME  AS CREATION_LOG_USER_NAME,"
      "CREATION_LOG_GROUP_NAME AS CREATION_LOG_GROUP_NAME,"
      "CREATION_LOG_HOST_NAME  AS CREATION_LOG_HOST_NAME,"
      "CREATION_LOG_TIME       AS CREATION_LOG_TIME,"

      "LAST_MOD_USER_NAME  AS LAST_MOD_USER_NAME,"
      "LAST_MOD_GROUP_NAME AS LAST_MOD_GROUP_NAME,"
      "LAST_MOD_HOST_NAME  AS LAST_MOD_HOST_NAME,"
      "LAST_MOD_TIME       AS LAST_MOD_TIME "
    "FROM ADMIN_HOST";
  SqliteStmt stmt(m_conn, sql);
  ColumnNameToIdx  nameToIdx;
  while(SQLITE_ROW == stmt.step()) {
    if(nameToIdx.empty()) {
      nameToIdx = stmt.getColumnNameToIdx();
    }
    common::dataStructures::AdminHost host;

    host.name = stmt.columnText(nameToIdx["HOST_NAME"]);
    host.comment = stmt.columnText(nameToIdx["COMMENT"]);

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = stmt.columnText(nameToIdx["CREATION_LOG_USER_NAME"]);
    creatorUI.group = stmt.columnText(nameToIdx["CREATION_LOG_GROUP_NAME"]);

    common::dataStructures::EntryLog creationLog;
    creationLog.user = creatorUI;
    creationLog.host = stmt.columnText(nameToIdx["CREATION_LOG_HOST_NAME"]);
    creationLog.time = stmt.columnUint64(nameToIdx["CREATION_LOG_TIME"]);

    host.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = stmt.columnText(nameToIdx["LAST_MOD_USER_NAME"]);
    updaterUI.group = stmt.columnText(nameToIdx["LAST_MOD_GROUP_NAME"]);

    common::dataStructures::EntryLog updateLog;
    updateLog.user = updaterUI;
    updateLog.host = stmt.columnText(nameToIdx["LAST_MOD_HOST_NAME"]);
    updateLog.time = stmt.columnUint64(nameToIdx["LAST_MOD_TIME"]);

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
void cta::catalogue::SqliteCatalogue::createStorageClass(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteStorageClass(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::catalogue::SqliteCatalogue::getStorageClasses(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::StorageClass>();}

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
void cta::catalogue::SqliteCatalogue::createTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteTapePool(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::catalogue::SqliteCatalogue::getTapePools(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::TapePool>();}

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
void cta::catalogue::SqliteCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteArchiveRoute(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::catalogue::SqliteCatalogue::getArchiveRoutes(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::ArchiveRoute>();}

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
void cta::catalogue::SqliteCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteLogicalLibrary(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::catalogue::SqliteCatalogue::getLogicalLibraries(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::LogicalLibrary>();}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::createTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::catalogue::SqliteCatalogue::getTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue) { return std::list<cta::common::dataStructures::Tape>();}

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
void cta::catalogue::SqliteCatalogue::createUser(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteUser(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group) {}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::catalogue::SqliteCatalogue::getUsers(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::User>();}

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
void cta::catalogue::SqliteCatalogue::createMountGroup(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::SqliteCatalogue::deleteMountGroup(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {}

//------------------------------------------------------------------------------
// getMountGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::MountGroup> cta::catalogue::SqliteCatalogue::getMountGroups(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::MountGroup>();}

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
void cta::catalogue::SqliteCatalogue::deleteDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::catalogue::SqliteCatalogue::getDedications(const common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::Dedication>();}

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
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::catalogue::SqliteCatalogue::getArchiveFiles(const common::dataStructures::SecurityIdentity &cliIdentity, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::catalogue::SqliteCatalogue::getArchiveFileSummary(const common::dataStructures::SecurityIdentity &cliIdentity, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile cta::catalogue::SqliteCatalogue::getArchiveFileById(const uint64_t id){
  return cta::common::dataStructures::ArchiveFile();
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
  const cta::common::dataStructures::TapeFileLocation tapeFileLocation) {
}

//------------------------------------------------------------------------------
// getCopyNbToTapePoolMap
//------------------------------------------------------------------------------
std::map<uint64_t,std::string> cta::catalogue::SqliteCatalogue::
  getCopyNbToTapePoolMap(const std::string &storageClass) {
  return std::map<uint64_t,std::string>();
}

//------------------------------------------------------------------------------
// getArchiveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::SqliteCatalogue::
  getArchiveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) {
  return common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// getRetrieveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::SqliteCatalogue::
  getRetrieveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) {
  return common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool cta::catalogue::SqliteCatalogue::isAdmin(
  const common::dataStructures::SecurityIdentity &cliIdentity) {
  return false;
}
