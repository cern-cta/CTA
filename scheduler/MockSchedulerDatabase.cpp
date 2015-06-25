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

#include "common/ArchiveDirIterator.hpp"
#include "common/exception/Exception.hpp"
#include "common/UserIdentity.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchiveToDirRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabase.hpp"
#include "scheduler/RetrieveFromTapeCopyRequest.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/SqliteColumnNameToIndex.hpp"
#include "scheduler/SQLiteStatementDeleter.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockSchedulerDatabase::MockSchedulerDatabase() {
  try {
    if(sqlite3_open(":memory:", &m_dbHandle)) {
      std::ostringstream msg;
      msg << "SQLite error: Can't open database: " <<
        sqlite3_errmsg(m_dbHandle);
      throw(exception::Exception(msg.str()));
    }
    char *zErrMsg = 0;
    if(SQLITE_OK != sqlite3_exec(m_dbHandle, "PRAGMA foreign_keys = ON;", 0, 0,
      &zErrMsg)) {
      std::ostringstream msg;
      msg << "SqliteDatabase() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(msg.str()));
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
      "TAPEPOOL     TEXT,"
      "UID               INTEGER,"
      "GID               INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME      INTEGER,"
      "COMMENT           TEXT,"
      "PRIMARY KEY (STORAGECLASS_NAME, COPYNB),"
      "FOREIGN KEY(STORAGECLASS_NAME) REFERENCES STORAGECLASS(NAME),"
      "FOREIGN KEY(TAPEPOOL)     REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE STORAGECLASS("
      "NAME           TEXT     PRIMARY KEY,"
      "NBCOPIES       INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT"
      ");"
    "CREATE TABLE TAPEPOOL("
      "NAME           TEXT     PRIMARY KEY,"
      "NBPARTIALTAPES INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT"
      ");"
    "CREATE TABLE TAPE("
      "VID                 TEXT,"
      "LOGICALLIBRARY TEXT,"
      "TAPEPOOL       TEXT,"
      "CAPACITY_BYTES      INTEGER,"
      "DATAONTAPE_BYTES    INTEGER,"
      "UID                 INTEGER,"
      "GID                 INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME        INTEGER,"
      "COMMENT             TEXT,"
      "PRIMARY KEY (VID),"
      "FOREIGN KEY (LOGICALLIBRARY) REFERENCES LOGICALLIBRARY(NAME),"
      "FOREIGN KEY (TAPEPOOL) REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE LOGICALLIBRARY("
      "NAME           TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (NAME)"
      ");"
    "CREATE TABLE ADMINUSER("
      "ADMIN_UID      INTEGER,"
      "ADMIN_GID      INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (ADMIN_UID,ADMIN_GID)"
      ");"
    "CREATE TABLE ADMINHOST("
      "NAME           TEXT,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "COMMENT        TEXT,"
      "PRIMARY KEY (NAME)"
      ");"
    "CREATE TABLE ARCHIVETOTAPECOPYREQUEST("
      "STATE          TEXT,"
      "REMOTEFILE     TEXT,"
      "ARCHIVEFILE    TEXT,"
      "TAPEPOOL       TEXT,"
      "COPYNB         INTEGER,"
      "PRIORITY       INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST              TEXT,"
      "CREATIONTIME   INTEGER,"
      "PRIMARY KEY (ARCHIVEFILE, COPYNB),"
      "FOREIGN KEY (TAPEPOOL) REFERENCES TAPEPOOL(NAME)"
      ");"
    "CREATE TABLE RETRIEVEFROMTAPECOPYREQUEST("
      "ARCHIVEFILE    TEXT,"
      "REMOTEFILE     TEXT,"
      "COPYNB         INTEGER,"
      "VID            TEXT,"
      "FSEQ           INTEGER,"
      "BLOCKID        INTEGER,"
      "PRIORITY       INTEGER,"
      "UID            INTEGER,"
      "GID            INTEGER,"
      "HOST           TEXT,"
      "CREATIONTIME   INTEGER,"
      "PRIMARY KEY (REMOTEFILE, VID, FSEQ),"
      "FOREIGN KEY (VID) REFERENCES TAPE(VID)"
      ");",
    0, 0, &zErrMsg);
  if(SQLITE_OK != rc) {    
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(msg.str()));
  }
}  

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockSchedulerDatabase::~MockSchedulerDatabase() throw() {
  sqlite3_close(m_dbHandle);
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::queue(const ArchiveToDirRequest &rqst) {
  for(auto itor = rqst.getArchiveToFileRequests().cbegin();
    itor != rqst.getArchiveToFileRequests().cend(); itor++) {
    queue(*itor);
  }
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::queue(const ArchiveToFileRequest &rqst) {
  const std::map<uint16_t, std::string> &copyNbToPoolMap =
    rqst.getCopyNbToPoolMap();
  for(auto itor = copyNbToPoolMap.begin(); itor != copyNbToPoolMap.end();
    itor++) {
    const uint16_t copyNb = itor->first;
    const std::string &tapePoolName = itor->second;
    queue(ArchiveToTapeCopyRequest(
      rqst.getRemoteFile(),
      rqst.getArchiveFile(),
      copyNb,
      tapePoolName,
      rqst.getPriority(),
      rqst.getRequester()));
  }
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::queue(const ArchiveToTapeCopyRequest &rqst) {
  char *zErrMsg = 0;
  const SecurityIdentity &requester = rqst.getRequester();
  std::ostringstream query;
  query << "INSERT INTO ARCHIVETOTAPECOPYREQUEST(STATE, REMOTEFILE,"
    " ARCHIVEFILE, TAPEPOOL, COPYNB, PRIORITY, UID, GID, CREATIONTIME) VALUES("
    << "'PENDING_NS_CREATION','" << rqst.getRemoteFile() << "','" <<
    rqst.getArchiveFile() << "','" << rqst.getTapePoolName() << "'," <<
    rqst.getCopyNb() << "," << rqst.getPriority() << "," <<
    requester.getUser().uid << "," << requester.getUser().gid << ","
    << (int)time(NULL) << ");";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Archive request for copy " << rqst.getCopyNb() <<
      " of archive file " << rqst.getArchiveFile() << " already exists";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchiveToTapeCopyRequest> >
  cta::MockSchedulerDatabase::getArchiveRequests() const {
  std::ostringstream query;
  std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > rqsts;
  query << "SELECT STATE, REMOTEFILE, ARCHIVEFILE, TAPEPOOL, COPYNB,"
    " PRIORITY, UID, GID, CREATIONTIME FROM ARCHIVETOTAPECOPYREQUEST"
    " ORDER BY ARCHIVEFILE;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    const SqliteColumnNameToIndex idx(statement.get());
    const std::string tapePoolName =
      (char *)sqlite3_column_text(statement.get(), idx("TAPEPOOL"));
    const TapePool tapePool = getTapePool(tapePoolName);
    const std::string remoteFile = (char *)sqlite3_column_text(statement.get(),
      idx("REMOTEFILE"));
    const std::string archiveFile = (char *)sqlite3_column_text(statement.get(),
      idx("ARCHIVEFILE"));
    const uint16_t copyNb = sqlite3_column_int(statement.get(), idx("COPYNB"));
    const std::string tapePolMName = (char *)sqlite3_column_text(
      statement.get(), idx("TAPEPOOL"));
    const uint16_t requesterUid = sqlite3_column_int(statement.get(),
      idx("UID"));
    const uint16_t requesterGid = sqlite3_column_int(statement.get(),
      idx("GID"));
    const std::map<uint16_t, std::string> copyNbToPoolMap;
    const uint64_t priority = sqlite3_column_int(statement.get(),
      idx("PRIORITY"));
    const UserIdentity requester(requesterUid, requesterGid);
    const std::string requesterHost = "requester_host";
    const SecurityIdentity requesterAndHost(requester, requesterHost);
    const time_t creationTime = sqlite3_column_int(statement.get(),
      idx("CREATIONTIME"));
    rqsts[tapePool].push_back(ArchiveToTapeCopyRequest(
      remoteFile,
      archiveFile,
      copyNb,
      tapePoolName,
      priority,
      requesterAndHost,
      creationTime
    ));
  }
  return rqsts;
}

//------------------------------------------------------------------------------
// getTapePool
//------------------------------------------------------------------------------
cta::TapePool cta::MockSchedulerDatabase::getTapePool(const std::string &name)
  const {
  std::ostringstream query;
  cta::TapePool pool;
  query << "SELECT NAME, NBPARTIALTAPES, UID, GID, HOST, CREATIONTIME, COMMENT"
    " FROM TAPEPOOL WHERE NAME='" << name << "';";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  switch(sqlite3_step(statement.get())) {
  case SQLITE_ROW:
    {
      SqliteColumnNameToIndex idx(statement.get());
      pool = TapePool(
        (char *)sqlite3_column_text(statement.get(),idx("NAME")),
        sqlite3_column_int(statement.get(),idx("NBPARTIALTAPES")),
        CreationLog(
          UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
            sqlite3_column_int(statement.get(),idx("GID"))),
          (char *)sqlite3_column_text(statement.get(),idx("HOST")),
          time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
          (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
        )
      );
    }
    break;
  case SQLITE_DONE:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - No tape pool found with name: " << name;
      throw(exception::Exception(msg.str()));
    }
    break;
  default:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error";
      throw(exception::Exception(msg.str()));
    }
  }
  return pool;
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::list<cta::ArchiveToTapeCopyRequest> cta::MockSchedulerDatabase::
  getArchiveRequests(const std::string &tapePoolName) const {
  std::ostringstream query;
  std::list<ArchiveToTapeCopyRequest> rqsts;
  query << "SELECT STATE, REMOTEFILE, ARCHIVEFILE, TAPEPOOL, COPYNB,"
    " PRIORITY, UID, GID, CREATIONTIME FROM ARCHIVETOTAPECOPYREQUEST"
    " WHERE TAPEPOOL='" << tapePoolName << "' ORDER BY ARCHIVEFILE;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    const SqliteColumnNameToIndex idx(statement.get());
    const std::string tapePoolName =
      (char *)sqlite3_column_text(statement.get(), idx("TAPEPOOL"));
    const TapePool tapePool = getTapePool(tapePoolName);
    const std::string remoteFile = (char *)sqlite3_column_text(statement.get(),
      idx("REMOTEFILE"));
    const std::string archiveFile = (char *)sqlite3_column_text(statement.get(),
      idx("ARCHIVEFILE"));
    const uint16_t copyNb = sqlite3_column_int(statement.get(), idx("COPYNB"));
    const std::string tapePolMName = (char *)sqlite3_column_text(
      statement.get(), idx("TAPEPOOL"));
    const uint16_t requesterUid = sqlite3_column_int(statement.get(),
      idx("UID"));
    const uint16_t requesterGid = sqlite3_column_int(statement.get(),
      idx("GID"));
    const std::map<uint16_t, std::string> copyNbToPoolMap;
    const uint64_t priority = sqlite3_column_int(statement.get(),
      idx("PRIORITY"));
    const UserIdentity requester(requesterUid, requesterGid);
    const std::string requesterHost = "requester_host";
    const SecurityIdentity requesterAndHost(requester, requesterHost);
    const time_t creationTime = sqlite3_column_int(statement.get(),
      idx("CREATIONTIME"));
    rqsts.push_back(ArchiveToTapeCopyRequest(
      remoteFile,
      archiveFile,
      copyNb,
      tapePoolName,
      priority,
      requesterAndHost,
      creationTime
    ));
  }
  return rqsts;
}

//------------------------------------------------------------------------------
// deleteArchiveRequest
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteArchiveRequest(
  const SecurityIdentity &requester,
  const std::string &archiveFile) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ARCHIVETOTAPECOPYREQUEST WHERE ARCHIVEFILE='" <<
    archiveFile << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Archive request for archive file " << archiveFile <<
      " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// markArchiveRequestForDeletion
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::markArchiveRequestForDeletion(
  const SecurityIdentity &requester,
  const std::string &archiveFile) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "UPDATE ARCHIVETOTAPECOPYREQUEST SET STATE='PENDING_NS_DELETION'"
    " WHERE ARCHIVEFILE='" << archiveFile << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "An archive requests for archive file " << archiveFile <<
      " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// fileEntryDeletedFromNS
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::fileEntryDeletedFromNS(
  const SecurityIdentity &requester, const std::string &archiveFile) {
  deleteArchiveRequest(requester, archiveFile);
}

//------------------------------------------------------------------------------
// fileEntryCreatedInNS
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::fileEntryCreatedInNS(
  const SecurityIdentity &requester,
  const std::string &archiveFile) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "UPDATE ARCHIVETOTAPECOPYREQUEST SET STATE='PENDING_MOUNT' WHERE"
    " STATE='PENDING_NS_CREATION' AND ARCHIVEFILE='" << archiveFile << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "There are no archive requests in status PENDING_MOUNT for archive"
      " file " << archiveFile;
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::queue(const RetrieveToDirRequest &rqst) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::queue(const RetrieveToFileRequest &rqst) {
/*
  char *zErrMsg = 0;
  const SecurityIdentity &requester = rqst.getRequester();
  std::ostringstream query;
  query << "INSERT INTO RETRIEVEFROMTAPECOPYREQUEST(ARCHIVEFILE, REMOTEFILE, COPYNB, VID, FSEQ, BLOCKID, PRIORITY, UID,"
    " GID, HOST, CREATIONTIME) VALUES(" << (int)cta::RetrieveFromTapeCopyRequestState::PENDING <<
    ",'" << rqst.getArchiveFile() << "','" << rqst.getRemoteFile() << "'," <<
    requester.getUser().uid << "," << requester.getUser().gid <<
    "," << (int)time(NULL) << ");";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
*/
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrieveFromTapeCopyRequest> >
  cta::MockSchedulerDatabase::getRetrieveRequests() const {
  return std::map<cta::Tape, std::list<cta::RetrieveFromTapeCopyRequest> >();
/*
  std::ostringstream query;
  std::map<cta::Tape, std::list<cta::RetrieveFromTapeCopyRequest> > map;
  query << "SELECT ARCHIVEFILE, REMOTEFILE, COPYNB, VID, FSEQ, BLOCKID,"
    " PRIORITY, UID, GID, CREATIONTIME FROM RETRIEVEFROMTAPECOPYREQUEST ORDER"
    " BY REMOTEFILE;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    const std::string vid = (char *)sqlite3_column_text(statement.get(),idx("VID"));
    const Tape tape = getTape(vid);
    const uint16_t requesterUid = sqlite3_column_int(statement.get(),
      idx("UID"));
    const uint16_t requesterGid = sqlite3_column_int(statement.get(),
      idx("GID"));
    UserIdentity requester(requesterUid, requesterGid);
    SecurityIdentity requesterAndHost(
    map[tape].push_back(RetrieveFromTapeCopyRequest(
      (char *)sqlite3_column_text(statement.get(),idx("ARCHIVEFILE")),
      (char *)sqlite3_column_text(statement.get(),idx("REMOTEFILE")),
      UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
      sqlite3_column_int(statement.get(),idx("GID"))),
      time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME")))
    ));
  }
  return map;
*/
}

//------------------------------------------------------------------------------
// getTape
//------------------------------------------------------------------------------
cta::Tape cta::MockSchedulerDatabase::getTape(const std::string &vid) const {
  std::ostringstream query;
  cta::Tape tape;
  query << "SELECT VID, LOGICALLIBRARY, TAPEPOOL, CAPACITY_BYTES,"
    " DATAONTAPE_BYTES, UID, GID, HOST, CREATIONTIME, COMMENT FROM TAPE WHERE VID='"
    << vid << "';";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  switch(sqlite3_step(statement.get())) {
  case SQLITE_ROW:
    {
      SqliteColumnNameToIndex idx(statement.get());
      tape = Tape(
        (char *)sqlite3_column_text(statement.get(),idx("VID")),
        (char *)sqlite3_column_text(statement.get(),idx("LOGICALLIBRARY")),
        (char *)sqlite3_column_text(statement.get(),idx("TAPEPOOL")),
        (uint64_t)sqlite3_column_int64(statement.get(),idx("CAPACITY_BYTES")),
        (uint64_t)sqlite3_column_int64(statement.get(),idx("DATAONTAPE_BYTES")),
        CreationLog(UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
            sqlite3_column_int(statement.get(),idx("GID"))),
          (char *)sqlite3_column_text(statement.get(),idx("COMMENT")),
          time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
          (char *)sqlite3_column_text(statement.get(),idx("COMMENT")))
      );
    }
    break;
  case SQLITE_DONE:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - No tape found with vid: " << vid;
      throw(exception::Exception(msg.str()));
    }
    break;
  default:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error";
      throw(exception::Exception(msg.str()));
    }
  }
  return tape;
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveFromTapeCopyRequest> cta::MockSchedulerDatabase::getRetrieveRequests(
  const std::string &vid) const {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}
  
//------------------------------------------------------------------------------
// deleteRetrieveRequest
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteRetrieveRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM RETRIEVEFROMTAPECOPYREQUEST WHERE REMOTEFILE='" << remoteFile << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Retrival job for remote file file " << remoteFile <<
      " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  const uint32_t adminUid = user.uid;
  const uint32_t adminGid = user.gid;
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ADMINUSER(ADMIN_UID, ADMIN_GID, UID, GID,"
    " CREATIONTIME, COMMENT) VALUES(" << adminUid << "," << adminGid << "," <<
    requester.getUser().uid << "," << requester.getUser().gid << ","
    << (int)time(NULL) << ",'" << comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Admin user uid=" << adminUid << " gid=" << adminGid <<
      " already exists";
    throw(exception::Exception(msg.str()));
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
  query << "DELETE FROM ADMINUSER WHERE ADMIN_UID=" << user.uid <<
    " AND ADMIN_GID=" << user.gid <<";";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(msg.str()));
  }
  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Admin user " << user.uid << ":" << user.gid <<
      " does not exist";
    throw(exception::Exception(msg.str()));
  }
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::MockSchedulerDatabase::getAdminUsers() const {
  std::ostringstream query;
  std::list<cta::AdminUser> list;
  query << "SELECT ADMIN_UID, ADMIN_GID, UID, GID, CREATIONTIME, COMMENT"
    " FROM ADMINUSER ORDER BY ADMIN_UID, ADMIN_GID;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    const UserIdentity user(sqlite3_column_int(statement.get(),idx("ADMIN_UID")),
      sqlite3_column_int(statement.get(),idx("ADMIN_GID")));
    const UserIdentity requester(sqlite3_column_int(statement.get(),idx("UID")),
      sqlite3_column_int(statement.get(),idx("GID")));
    list.push_back(AdminUser(
      user,
      requester,
      (char *)sqlite3_column_text(statement.get(),idx("COMMENT")),
      time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME")))
    ));
  }
  return list;
}

//------------------------------------------------------------------------------
// assertIsAdminOnAdminHost
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::assertIsAdminOnAdminHost(
  const SecurityIdentity &id) const {
  assertIsAdmin(id.getUser());
  assertIsAdminHost(id.getHost());
}

//------------------------------------------------------------------------------
// assertIsAdmin
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::assertIsAdmin(const UserIdentity &user)
  const {
  const uint32_t adminUid = user.uid;
  const uint32_t adminGid = user.gid;
  std::ostringstream query;
  query << "SELECT COUNT(*) FROM ADMINUSER WHERE ADMIN_UID=" << adminUid <<
    " AND ADMIN_GID=" << adminGid << ";";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  int count = 0;
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    count = sqlite3_column_int(statement.get(), 0);
  }

  if(0 >= count) {
    std::ostringstream msg;
    msg << "User uid=" << adminUid << " gid=" << adminGid <<
      " is not an administrator";
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// assertIsAdminHost
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::assertIsAdminHost(const std::string &host)
  const {
  std::ostringstream query;
  query << "SELECT COUNT(*) FROM ADMINHOST WHERE NAME='" << host << "';";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  int count = 0;
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    count = sqlite3_column_int(statement.get(), 0);
  }

  if(0 >= count) {
    std::ostringstream msg;
    msg << "Host " << host << " is not an administration host";
    throw exception::Exception(msg.str());
  }
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
    " VALUES('" << hostName << "',"<< requester.getUser().uid << "," <<
    requester.getUser().gid << "," << (int)time(NULL) << ",'" << comment <<
    "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Admin host " << hostName << " already exists";
    throw(exception::Exception(msg.str()));
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
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Admin host " << hostName << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::MockSchedulerDatabase::getAdminHosts() const {
  std::ostringstream query;
  std::list<cta::AdminHost> list;
  query << "SELECT NAME, UID, GID, CREATIONTIME, COMMENT"
    " FROM ADMINHOST ORDER BY NAME;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    const UserIdentity requester(sqlite3_column_int(statement.get(),idx("UID")),
      sqlite3_column_int(statement.get(),idx("GID")));
    list.push_back(AdminHost(
      (char *)sqlite3_column_text(statement.get(),idx("NAME")),
      requester,
      (char *)sqlite3_column_text(statement.get(),idx("COMMENT")),
      time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME")))
    ));
  }
  return list;
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createStorageClass(
  const std::string &name,
  const uint16_t nbCopies, const CreationLog &creationLog) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO STORAGECLASS(NAME, NBCOPIES, UID, GID, HOST, CREATIONTIME,"
    " COMMENT) VALUES('" << name << "'," << (int)nbCopies << "," <<
    creationLog.user.uid << "," << creationLog.user.gid << ", '" <<
    creationLog.host << "', " <<
    creationLog.time << ",'" << creationLog.comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
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
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Storage class " << name << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::MockSchedulerDatabase::getStorageClasses()
  const {
  std::ostringstream query;
  std::list<cta::StorageClass> classes;
  query << "SELECT NAME, NBCOPIES, UID, GID, HOST, CREATIONTIME, COMMENT FROM"
    " STORAGECLASS ORDER BY NAME;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    classes.push_back(StorageClass(
      (char *)sqlite3_column_text(statement.get(),idx("NAME")),
      sqlite3_column_int(statement.get(),idx("NBCOPIES")),
      CreationLog(
        UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
          sqlite3_column_int(statement.get(),idx("GID"))),
        (char *)sqlite3_column_text(statement.get(),idx("HOST")),
        time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
      )
    ));
  }
  return classes;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
cta::StorageClass cta::MockSchedulerDatabase::getStorageClass(
  const std::string &name) const {
  std::ostringstream query;
  query << "SELECT NAME, NBCOPIES, UID, GID, HOST, CREATIONTIME, COMMENT"
    " FROM STORAGECLASS WHERE NAME='" << name << "';";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  StorageClass storageClass;
  switch(sqlite3_step(statement.get())) {
  case SQLITE_ROW:
    {
      SqliteColumnNameToIndex idx(statement.get());
      storageClass = cta::StorageClass(
        (char *)sqlite3_column_text(statement.get(),idx("NAME")),
        sqlite3_column_int(statement.get(),idx("NBCOPIES")),
        CreationLog(
          UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
            sqlite3_column_int(statement.get(),idx("GID"))),
          (char *)sqlite3_column_text(statement.get(),idx("HOST")),
          time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
          (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
        )
      );
    }
    break;
  case SQLITE_DONE:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - No storage class found with name " << name;
      throw(exception::Exception(msg.str()));
    }
    break;
  default:
    {
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error";
      throw(exception::Exception(msg.str()));
    }
  }
  return storageClass;
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createTapePool(
  const std::string &name,
  const uint32_t nbPartialTapes,
  const CreationLog& creationLog) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO TAPEPOOL(NAME, NBPARTIALTAPES, UID, GID, HOST, CREATIONTIME,"
    " COMMENT) VALUES('" << name << "'," << (int)nbPartialTapes << "," <<
    creationLog.user.uid << "," << creationLog.user.uid << "," <<
    "'" << creationLog.host << "', " << 
    (int)time(NULL) << ",'" << creationLog.comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM TAPEPOOL WHERE NAME='" << name << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Tape pool " << name << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::MockSchedulerDatabase::getTapePools() const {
  std::ostringstream query;
  std::list<cta::TapePool> pools;
  query << "SELECT NAME, NBPARTIALTAPES, UID, GID, HOST, CREATIONTIME, COMMENT FROM"
    " TAPEPOOL ORDER BY NAME;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    pools.push_back(TapePool(
      (char *)sqlite3_column_text(statement.get(),idx("NAME")),
      sqlite3_column_int(statement.get(),idx("NBPARTIALTAPES")),
      CreationLog(
          UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
            sqlite3_column_int(statement.get(),idx("GID"))),
          (char *)sqlite3_column_text(statement.get(),idx("HOST")),
          time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
          (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
        )
    ));
  }
  return pools;
}

//------------------------------------------------------------------------------
// createArchivalRoute
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createArchivalRoute(
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const CreationLog &creationLog) {
  const std::list<ArchivalRoute> routes = getArchivalRoutesWithoutChecks(
    storageClassName);
  const StorageClass storageClass = getStorageClass(storageClassName);
  if(routes.size() >= storageClass.getNbCopies()) {
    std::ostringstream msg;
    msg << "Too many archival routes for storage class "
      << storageClassName;
    throw(exception::Exception(msg.str()));
  }

  assertTapePoolIsNotAlreadyADestination(routes, tapePoolName);

  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ARCHIVALROUTE(STORAGECLASS_NAME, COPYNB, TAPEPOOL,"
    " UID, GID, HOST, CREATIONTIME, COMMENT) VALUES('" << storageClassName << "'," <<
    (int)copyNb << ",'" << tapePoolName << "'," << creationLog.user.uid <<
    "," << creationLog.user.gid << ",'" << creationLog.host << 
    "'," << (int)time(NULL) << ",'" << creationLog.comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Archival route for storage class " << storageClassName <<
      " copy number " << copyNb << " already exists";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// assertTapePoolIsNotAlreadyADestination
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::assertTapePoolIsNotAlreadyADestination(
  const std::list<ArchivalRoute> &routes,
  const std::string &tapePoolName) const {
  for(auto itor = routes.begin(); itor != routes.end(); itor++) {
    if(tapePoolName == itor->getTapePoolName()) {
      std::ostringstream msg;
      msg << "Tape pool " << tapePoolName << " is already an archival"
        " destination for storage class " << itor->getStorageClassName();
      throw exception::Exception(msg.str());
    }
  }
}

//------------------------------------------------------------------------------
// deleteArchivalRoute
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ARCHIVALROUTE WHERE STORAGECLASS_NAME='" <<
    storageClassName << "' AND COPYNB=" << (int)copyNb << ";";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Archival route for storage class " << storageClassName << 
      " and copy number " << copyNb << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::MockSchedulerDatabase::getArchivalRoutes()
  const {
  std::ostringstream query;
  std::list<cta::ArchivalRoute> routes;
  query << "SELECT STORAGECLASS_NAME, COPYNB, TAPEPOOL, UID, GID, HOST,"
    " CREATIONTIME, COMMENT FROM ARCHIVALROUTE ORDER BY STORAGECLASS_NAME,"
    " COPYNB;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    routes.push_back(ArchivalRoute(
      (char *)sqlite3_column_text(statement.get(),idx("STORAGECLASS_NAME")),
      sqlite3_column_int(statement.get(),idx("COPYNB")),
      (char *)sqlite3_column_text(statement.get(),idx("TAPEPOOL")),
      CreationLog(
        UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
          sqlite3_column_int(statement.get(),idx("GID"))),
        (char *)sqlite3_column_text(statement.get(),idx("HOST")),
        time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
      )
    ));
  }
  return routes;
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::MockSchedulerDatabase::getArchivalRoutes(
  const std::string &storageClassName) const {
  std::ostringstream query;
  const std::list<cta::ArchivalRoute> routes = getArchivalRoutesWithoutChecks(
    storageClassName);

  const StorageClass storageClass = getStorageClass(storageClassName);
  if(routes.size() < storageClass.getNbCopies()) {
    std::ostringstream msg;
    msg << "Incomplete archival routes for storage class "
      << storageClassName;
    throw(exception::Exception(msg.str()));
  }

  return routes;
}

//------------------------------------------------------------------------------
// getArchivalRoutesWithoutChecks
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::MockSchedulerDatabase::
  getArchivalRoutesWithoutChecks(const std::string &storageClassName) const {
  std::ostringstream query;
  std::list<cta::ArchivalRoute> routes;
  query << "SELECT STORAGECLASS_NAME, COPYNB, TAPEPOOL, UID, GID, HOST,"
    " CREATIONTIME, COMMENT FROM ARCHIVALROUTE WHERE STORAGECLASS_NAME='" <<
    storageClassName << "' ORDER BY STORAGECLASS_NAME, COPYNB;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    routes.push_back(ArchivalRoute(
      (char *)sqlite3_column_text(statement.get(),idx("STORAGECLASS_NAME")),
      sqlite3_column_int(statement.get(),idx("COPYNB")),
      (char *)sqlite3_column_text(statement.get(),idx("TAPEPOOL")),
      CreationLog(
        UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
          sqlite3_column_int(statement.get(),idx("GID"))),
        (char *)sqlite3_column_text(statement.get(),idx("HOST")),
        time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
      )
    ));
  }
  return routes;
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createLogicalLibrary(
  const std::string &name,
  const cta::CreationLog& creationLog) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO LOGICALLIBRARY(NAME, UID, GID, HOST, CREATIONTIME, COMMENT)"
    << " VALUES('" << name << "',"<< creationLog.user.uid << ","
    << creationLog.user.gid << "," 
    << " '" << creationLog.host << "', "
    << creationLog.time << ",'" << creationLog.comment <<
    "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM LOGICALLIBRARY WHERE NAME='" << name << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Logical library " << name << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockSchedulerDatabase::getLogicalLibraries()
  const {
  std::ostringstream query;
  std::list<cta::LogicalLibrary> list;
  query << "SELECT NAME, UID, GID, HOST, CREATIONTIME, COMMENT"
    " FROM LOGICALLIBRARY ORDER BY NAME;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    list.push_back(LogicalLibrary(
      (char *)sqlite3_column_text(statement.get(),idx("NAME")),
      CreationLog(
        UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
          sqlite3_column_int(statement.get(),idx("GID"))),
        (char *)sqlite3_column_text(statement.get(),idx("HOST")),
        time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT"))
      )
    ));
  }
  return list;
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::createTape(
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const CreationLog &creationLog) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO TAPE(VID, LOGICALLIBRARY, TAPEPOOL,"
    " CAPACITY_BYTES, DATAONTAPE_BYTES, UID, GID, HOST, CREATIONTIME, COMMENT)"
    " VALUES('" << vid << "','" << logicalLibraryName << "','" << tapePoolName
    << "',"<< (long unsigned int)capacityInBytes << ",0," <<
    creationLog.user.uid << "," << creationLog.user.gid << ","
    << "'" << creationLog.host << "', " << creationLog.time << ",'" 
    << creationLog.comment << "');";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::MockSchedulerDatabase::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM TAPE WHERE NAME='" << vid << "';";
  if(SQLITE_OK != sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0,
    &zErrMsg)) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(exception::Exception(msg.str()));
  }

  const int nbRowsModified = sqlite3_changes(m_dbHandle);
  if(0 >= nbRowsModified) {
    std::ostringstream msg;
    msg << "Tape " << vid << " does not exist";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::MockSchedulerDatabase::getTapes() const {
  std::ostringstream query;
  std::list<cta::Tape> tapes;
  query << "SELECT VID, LOGICALLIBRARY, TAPEPOOL, CAPACITY_BYTES,"
    " DATAONTAPE_BYTES, UID, GID, HOST, CREATIONTIME, COMMENT"
    " FROM TAPE ORDER BY VID;";
  sqlite3_stmt *s = NULL;
  const int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &s, 0 );
  std::unique_ptr<sqlite3_stmt, SQLiteStatementDeleter> statement(s);
  if(SQLITE_OK != rc){    
      std::ostringstream msg;
      msg << __FUNCTION__ << " - SQLite error: " << sqlite3_errmsg(m_dbHandle);
    throw(exception::Exception(msg.str()));
  }
  while(SQLITE_ROW == sqlite3_step(statement.get())) {
    SqliteColumnNameToIndex idx(statement.get());
    tapes.push_back(Tape(
      (char *)sqlite3_column_text(statement.get(),idx("VID")),
      (char *)sqlite3_column_text(statement.get(),idx("LOGICALLIBRARY")),
      (char *)sqlite3_column_text(statement.get(),idx("TAPEPOOL")),
      (uint64_t)sqlite3_column_int64(statement.get(),idx("CAPACITY_BYTES")),
      (uint64_t)sqlite3_column_int64(statement.get(),idx("DATAONTAPE_BYTES")),
      CreationLog(UserIdentity(sqlite3_column_int(statement.get(),idx("UID")),
          sqlite3_column_int(statement.get(),idx("GID"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT")),
        time_t(sqlite3_column_int(statement.get(),idx("CREATIONTIME"))),
        (char *)sqlite3_column_text(statement.get(),idx("COMMENT")))
    ));
  }
  return tapes;
}
