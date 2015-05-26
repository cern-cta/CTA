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

#include "common/exception/Exception.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalJob.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/DirIterator.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabase.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/SqliteColumnNameToIndex.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"
#include "scheduler/UserIdentity.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockSchedulerDatabase::MockSchedulerDatabase() {
  try {
    if(sqlite3_open(":memory:", &m_dbHandle)) {
      std::ostringstream message;
      message << "SQLite error: Can't open database: " <<
        sqlite3_errmsg(m_dbHandle);
      throw(exception::Exception(message.str()));
    }
    char *zErrMsg = 0;
    if(SQLITE_OK != sqlite3_exec(m_dbHandle, "PRAGMA foreign_keys = ON;", 0, 0,
      &zErrMsg)) {
      std::ostringstream message;
      message << "SqliteDatabase() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
    }
    createSchema();
  } catch (...) {
    sqlite3_close(m_dbHandle);
    throw;
  }
}

//------------------------------------------------------------------------------
// createSchema
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createSchema() {  
  char *zErrMsg = 0;
  const int rc = sqlite3_exec(m_dbHandle,
    "CREATE TABLE ARCHIVALROUTE("
      "STORAGECLASS_NAME TEXT,"
      "COPYNB            INTEGER,"
      "TAPEPOOL_NAME     TEXT,"
      "UID               INTEGER,"
      "GID               INTEGER,"
      "CREATIONTIME      INTEGER,"
      "COMMENT           TEXT,"
      "PRIMARY KEY (STORAGECLASS_NAME, COPYNB),"
      "FOREIGN KEY(STORAGECLASS_NAME) REFERENCES STORAGECLASS(NAME),"
      "FOREIGN KEY(TAPEPOOL_NAME)     REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE STORAGECLASS("
      "NAME           TEXT     PRIMARY KEY,"
      "NBCOPIES       INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT"
      ");"
    "CREATE TABLE TAPEPOOL("
      "NAME           TEXT     PRIMARY KEY,"
      "NBPARTIALTAPES INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT"
      ");"
    "CREATE TABLE TAPE("
      "VID                 TEXT,"
      "LOGICALLIBRARY_NAME TEXT,"
      "TAPEPOOL_NAME       TEXT,"
      "CAPACITY_BYTES      INTEGER,"
      "DATAONTAPE_BYTES    INTEGER,"
      "UID                 INTEGER,"
      "GID                 INTEGER,"
      "CREATIONTIME        INTEGER,"
      "COMMENT             TEXT,"
      "PRIMARY KEY (VID),"
      "FOREIGN KEY (LOGICALLIBRARY_NAME) REFERENCES LOGICALLIBRARY(NAME),"
      "FOREIGN KEY (TAPEPOOL_NAME) REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE LOGICALLIBRARY("
      "NAME           TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (NAME)"
      ");"
    "CREATE TABLE ADMINUSER("
      "ADMIN_UID      INTEGER,"
      "ADMIN_GID      INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (ADMIN_UID,ADMIN_GID)"
      ");"
    "CREATE TABLE ADMINHOST("
      "NAME           TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (NAME)"
      ");"
    "CREATE TABLE ARCHIVALJOB("
      "STATE          INTEGER,"
      "SRCURL         TEXT,"
      "DSTPATH        TEXT,"
      "TAPEPOOL_NAME  TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "PRIMARY KEY (DSTPATH, TAPEPOOL_NAME),"
      "FOREIGN KEY (TAPEPOOL_NAME) REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE RETRIEVALJOB("
      "STATE          INTEGER,"
      "SRCPATH        TEXT,"
      "DSTURL         TEXT,"
      "VID            TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "CREATIONTIME   INTEGER,"
      "PRIMARY KEY (DSTURL, VID),"
      "FOREIGN KEY (VID) REFERENCES TAPE(VID)"
      ");",
    0, 0, &zErrMsg);
  if(SQLITE_OK != rc) {    
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockSchedulerDatabase::~MockSchedulerDatabase() throw() {
  sqlite3_close(m_dbHandle);
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ADMINUSER(ADMIN_UID, ADMIN_GID, UID, GID,"
    " CREATIONTIME, COMMENT) VALUES(" << user.getUid() << "," << user.getGid()
    << "," << requester.user.getUid() << "," << requester.user.getGid() << ","
    << (int)time(NULL) << ",'" << comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream message;
    message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(message.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Admin user " << user.getUid() << ":" << user.getGid() <<
      " already exists";
    throw(exception::Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ADMINUSER WHERE ADMIN_UID=" << user.getUid() <<
    " AND ADMIN_GID=" << user.getGid() <<";";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Admin user " << user.getUid() << ":" << user.getGid() <<
      " does not exist";
    throw(exception::Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::MockSchedulerDatabase::getAdminUsers(
  const SecurityIdentity &requester) const {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::AdminUser> list;
  query << "SELECT ADMIN_UID, ADMIN_GID, UID, GID, CREATIONTIME, COMMENT"
    " FROM ADMINUSER ORDER BY ADMIN_UID, ADMIN_GID;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(exception::Exception(message.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement)) {
    SqliteColumnNameToIndex idx(statement);
    const UserIdentity user(sqlite3_column_int(statement,idx("ADMIN_UID")),
      sqlite3_column_int(statement,idx("ADMIN_GID")));
    const UserIdentity creator(sqlite3_column_int(statement,idx("UID")),
            sqlite3_column_int(statement,idx("GID")));
    list.push_back(cta::AdminUser(
      user,
      creator,
      std::string((char *)sqlite3_column_text(statement,idx("COMMENT"))),
      time_t(sqlite3_column_int(statement,idx("CREATIONTIME")))
    ));
  }
  sqlite3_finalize(statement);
  return list;
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ADMINHOST(NAME, UID, GID, CREATIONTIME, COMMENT)"
    " VALUES('" << hostName << "',"<< requester.user.getUid() << "," <<
    requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment <<
    "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream message;
    message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(message.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Admin host " << hostName << " already exists";
    throw(exception::Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ADMINHOST WHERE NAME='" << hostName << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Admin host " << hostName << " does not exist";
    throw(exception::Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::MockSchedulerDatabase::getAdminHosts(
  const SecurityIdentity &requester) const {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::AdminHost> list;
  query << "SELECT NAME, UID, GID, CREATIONTIME, COMMENT FROM ADMINHOST ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(exception::Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    SqliteColumnNameToIndex idx(statement);
    const UserIdentity creator(sqlite3_column_int(statement,idx("UID")),sqlite3_column_int(statement,idx("GID")));
    list.push_back(cta::AdminHost(
      std::string((char *)sqlite3_column_text(statement,idx("NAME"))),
      creator,
      std::string((char *)sqlite3_column_text(statement,idx("COMMENT"))),
      time_t(sqlite3_column_int(statement,idx("CREATIONTIME")))
    ));
  }
  sqlite3_finalize(statement);
  return list;
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createStorageClass(
  const SecurityIdentity &requester, const std::string &name,
  const uint16_t nbCopies, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO STORAGECLASS(NAME, NBCOPIES, UID, GID, CREATIONTIME,"
    " COMMENT) VALUES('" << name << "'," << (int)nbCopies << "," <<
    requester.user.getUid() << "," << requester.user.getGid() << "," <<
    (int)time(NULL) << ",'" << comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Storage class " << name << " already exists";
    throw(exception::Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM STORAGECLASS WHERE NAME='" << name << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(message.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 > nbRowsModified) {
    std::ostringstream message;
    message << "Storage class " << name << " does not exist";
    throw(exception::Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::MockSchedulerDatabase::getStorageClasses(
  const SecurityIdentity &requester) const {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::StorageClass> classes;
  query << "SELECT NAME, NBCOPIES, UID, GID, CREATIONTIME, COMMENT FROM"
    " STORAGECLASS ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){
      std::ostringstream message;
      message << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(exception::Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    SqliteColumnNameToIndex idx(statement);
    classes.push_back(cta::StorageClass(
            std::string((char *)sqlite3_column_text(statement,idx("NAME"))),
            sqlite3_column_int(statement,idx("NBCOPIES")),
            cta::UserIdentity(sqlite3_column_int(statement,idx("UID")),
            sqlite3_column_int(statement,idx("GID"))),
            std::string((char *)sqlite3_column_text(statement,idx("COMMENT"))),
            time_t(sqlite3_column_int(statement,idx("CREATIONTIME")))
      ));
  }
  sqlite3_finalize(statement);
  return classes;
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  //m_db.insertTapePool(requester, name, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
  //m_db.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::MockSchedulerDatabase::getTapePools(
  const SecurityIdentity &requester) const {
  //return m_db.selectAllTapePools(requester);
  return std::list<cta::TapePool>();
}

//------------------------------------------------------------------------------
// createArchivalRoute
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  //return m_db.insertArchivalRoute(requester, storageClassName, copyNb,
  //  tapePoolName, comment);
}

//------------------------------------------------------------------------------
// deleteArchivalRoute
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  //return m_db.deleteArchivalRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::MockSchedulerDatabase::getArchivalRoutes(
  const SecurityIdentity &requester) const {
  //return m_db.selectAllArchivalRoutes(requester);
  return std::list<cta::ArchivalRoute>();
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  //m_db.insertLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  //m_db.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockSchedulerDatabase::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  //return m_db.selectAllLogicalLibraries(requester);
  return std::list<cta::LogicalLibrary>();
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
  //m_db.insertTape(requester, vid, logicalLibraryName, tapePoolName,
  //capacityInBytes, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  //m_db.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::MockSchedulerDatabase::getTapes(
  const SecurityIdentity &requester) const {
  //return m_db.selectAllTapes(requester);
  return std::list<cta::Tape>();
}
