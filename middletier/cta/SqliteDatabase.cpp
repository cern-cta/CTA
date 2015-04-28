#include "cta/Exception.hpp"
#include "cta/SqliteDatabase.hpp"
#include "cta/Utils.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// functor that given a string returns the index of the corresponding column or an exception
//------------------------------------------------------------------------------
struct cm {  
  cm(sqlite3_stmt *statement) {
    int col_count = sqlite3_column_count(statement);
    if(col_count==0) {
      std::ostringstream message;
      message << "cm() - SQLite error: sqlite3_column_count() returned 0";
      throw(cta::Exception(message.str()));
    }
    for(int i=0; i<col_count; i++) {
      const char *col_name = sqlite3_column_origin_name(statement,i);
      if(col_name == NULL) {
        std::ostringstream message;
        message << "cm() - SQLite error: sqlite3_column_origin_name() returned NULL";
        throw(cta::Exception(message.str()));
      }
      m_col_map[std::string(col_name)] = i;
    }
  }
  int operator()(const std::string &col_name) {
    std::map<std::string, int>::iterator it = m_col_map.find(col_name);
    if(it != m_col_map.end())
    {
      return it->second;
    }
    else {
      std::ostringstream message;
      message << "cm() - column " << col_name << " does not exist";
      throw(cta::Exception(message.str()));
    }
  }
private:
  std::map<std::string, int> m_col_map;
};

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SqliteDatabase::SqliteDatabase() {
  try {
    int rc = sqlite3_open(":memory:", &m_dbHandle);
    if(rc) {
      std::ostringstream message;
      message << "SQLite error: Can't open database: " << sqlite3_errmsg(m_dbHandle);
      throw(Exception(message.str()));
    }
    char *zErrMsg = 0;
    rc = sqlite3_exec(m_dbHandle, "PRAGMA foreign_keys = ON;", 0, 0, &zErrMsg);
    if(rc!=SQLITE_OK) {    
      std::ostringstream message;
      message << "SqliteDatabase() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
    }
    createSchema();
  } catch (...) {
    sqlite3_close(m_dbHandle);
    throw;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::SqliteDatabase::~SqliteDatabase() throw() {
  sqlite3_close(m_dbHandle);
}

//------------------------------------------------------------------------------
// createSchema
//------------------------------------------------------------------------------
void cta::SqliteDatabase::createSchema() {  
  char *zErrMsg = 0;
  int rc = sqlite3_exec(m_dbHandle,
          "CREATE TABLE ARCHIVEROUTE("
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
            "NBDRIVES       INTEGER,"
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
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "createRetrievalJobTable() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}  
  
//------------------------------------------------------------------------------
// insertTape
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO TAPE VALUES('" << vid << "','" << logicalLibraryName << "','" << tapePoolName << "',"<< (long unsigned int)capacityInBytes << ",0,"<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertTape() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertAdminUser
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertAdminUser(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ADMINUSER VALUES(" << user.getUid() << "," << user.getGid() << ","<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertAdminUser() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertAdminHost
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertAdminHost(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ADMINHOST VALUES('" << hostName << "',"<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertAdminHost() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertArchivalJob
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertArchivalJob(const SecurityIdentity &requester, const std::string &tapepool, const std::string &srcUrl, const std::string &dstPath) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ARCHIVALJOB VALUES(" << (int)cta::ArchivalJobState::PENDING << ",'" << srcUrl << "','" << dstPath << "','" << tapepool << "',"<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ");";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertArchivalJob() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      std::cout << message.str() << std::endl;
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertRetrievalJob
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertRetrievalJob(const SecurityIdentity &requester, const std::string &vid, const std::string &srcPath, const std::string &dstUrl) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO RETRIEVALJOB VALUES(" << (int)cta::RetrievalJobState::PENDING << ",'" << srcPath << "','" << dstUrl << "','" << vid << "',"<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ");";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertRetrievalJob() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO LOGICALLIBRARY VALUES('" << name << "',"<< requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertLogicalLibrary() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// insertTapePool
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertTapePool(const SecurityIdentity &requester, const std::string &name, const uint16_t nbDrives, const uint32_t nbPartialTapes, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO TAPEPOOL VALUES('" << name << "'," << (int)nbDrives << "," << (int)nbPartialTapes << "," << requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
    std::ostringstream message;
    message << "insertTapePool() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// insertStorageClass
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO STORAGECLASS VALUES('" << name << "'," << (int)nbCopies << "," << requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertStorageClass() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// insertArchiveRoute
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO ARCHIVEROUTE VALUES('" << storageClassName << "'," << (int)copyNb << ",'" << tapePoolName << "'," << requester.user.getUid() << "," << requester.user.getGid() << "," << (int)time(NULL) << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "insertArchiveRoute() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteTapePool(const SecurityIdentity &requester, const std::string &name) {
  checkTapePoolExists(name);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM TAPEPOOL WHERE NAME='" << name << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteTapePool() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteStorageClass(const SecurityIdentity &requester, const std::string &name) {
  checkStorageClassExists(name);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM STORAGECLASS WHERE NAME='" << name << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteStorageClass() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb) {
  checkArchiveRouteExists(storageClassName, copyNb);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ARCHIVEROUTE WHERE STORAGECLASS_NAME='" << storageClassName << "' AND COPYNB=" << (int)copyNb << ";";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteMigrationRoute() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteTape(const SecurityIdentity &requester, const std::string &vid) {
  checkTapeExists(vid);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM TAPE WHERE NAME='" << vid << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteTape() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteAdminUser(const SecurityIdentity &requester, const UserIdentity &user) {
  checkAdminUserExists(user);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ADMINUSER WHERE ADMIN_UID=" << user.getUid() << " AND ADMIN_GID=" << user.getGid() <<";";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteAdminUser() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteAdminHost(const SecurityIdentity &requester, const std::string &hostName) {
  checkAdminHostExists(hostName);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ADMINHOST WHERE NAME='" << hostName << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteAdminHost() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteArchivalJob(const SecurityIdentity &requester, const std::string &dstPath) {
  checkArchivalJobExists(dstPath);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM ARCHIVALJOB WHERE DSTPATH='" << dstPath << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteArchivalJob() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteRetrievalJob
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteRetrievalJob(const SecurityIdentity &requester, const std::string &dstUrl) {
  checkRetrievalJobExists(dstUrl);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM RETRIEVALJOB WHERE DSTURL='" << dstUrl << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteRetrievalJob() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name) {
  checkLogicalLibraryExists(name);
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM LOGICALLIBRARY WHERE NAME='" << name << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "deleteLogicalLibrary() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// checkTapeExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkTapeExists(const std::string &vid){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM TAPE WHERE NAME='" << vid << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkTapeExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "TAPE: " << vid << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkTapeExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
  
//------------------------------------------------------------------------------
// checkAdminUserExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkAdminUserExists(const cta::UserIdentity &user){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM ADMINUSER WHERE ADMIN_UID=" << user.getUid() << " AND ADMIN_GID=" << user.getGid() <<";";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkAdminUserExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "ADMINUSER: " << user.getUid() << ":" << user.getGid()<< " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkAdminUserExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
  
//------------------------------------------------------------------------------
// checkAdminHostExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkAdminHostExists(const std::string &hostName){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM ADMINHOST WHERE NAME='" << hostName << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkAdminHostExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "ADMINHOST: " << hostName << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkAdminHostExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
  
//------------------------------------------------------------------------------
// checkArchivalJobExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkArchivalJobExists(const std::string &dstPath){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM ARCHIVALJOB WHERE DSTPATH='" << dstPath << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkArchivalJobExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "ARCHIVALJOB: " << dstPath << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkArchivalJobExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);  
}
  
//------------------------------------------------------------------------------
// checkRetrievalJobExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkRetrievalJobExists(const std::string &dstUrl){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM RETRIEVALJOB WHERE DSTURL='" << dstUrl << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkRetrievalJobExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "RETRIEVALJOB: " << dstUrl << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkRetrievalJobExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
  
//------------------------------------------------------------------------------
// checkLogicalLibraryExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkLogicalLibraryExists(const std::string &name){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM LOGICALLIBRARY WHERE NAME='" << name << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkLogicalLibraryExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "LOGICALLIBRARY: " << name << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkLogicalLibraryExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}

//------------------------------------------------------------------------------
// checkTapePoolExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkTapePoolExists(const std::string &name){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM TAPEPOOL WHERE NAME='" << name << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkTapePoolExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    sqlite3_finalize(statement);
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "TAPEPOOL: " << name << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkTapePoolExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
 
//------------------------------------------------------------------------------
// checkStorageClassExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkStorageClassExists(const std::string &name){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM STORAGECLASS WHERE NAME='" << name << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkStorageClassExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "STORAGECLASS: " << name << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkStorageClassExists() - SQLite error: " << res << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}
 
//------------------------------------------------------------------------------
// checkArchiveRouteExists
//------------------------------------------------------------------------------
void cta::SqliteDatabase::checkArchiveRouteExists(const std::string &name, const uint16_t copyNb){
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT * FROM ARCHIVEROUTE WHERE STORAGECLASS_NAME='" << name << "' AND COPYNB=" << (int)copyNb << ";";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "checkArchiveRouteExists() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    return;
  } 
  else if(res==SQLITE_DONE){
    std::ostringstream message;
    message << "ARCHIVEROUTE: " << name << " with COPYNB: " << (int)copyNb << " does not exist";
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  else {
    std::ostringstream message;
    message << "checkArchiveRouteExists() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));    
  }
  sqlite3_finalize(statement);
}

//------------------------------------------------------------------------------
// selectAllTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::SqliteDatabase::selectAllTapePools(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::TapePool> pools;
  query << "SELECT NAME, NBDRIVES, NBPARTIALTAPES, UID, GID, CREATIONTIME, COMMENT FROM TAPEPOOL ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllTapePools() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    pools.push_back(cta::TapePool(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            sqlite3_column_int(statement,cm("NBDRIVES")),
            sqlite3_column_int(statement,cm("NBPARTIALTAPES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),
            sqlite3_column_int(statement,cm("GID"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME")))
      ));
  }
  sqlite3_finalize(statement);  
  return pools;
}

//------------------------------------------------------------------------------
// selectAllStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::SqliteDatabase::selectAllStorageClasses(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::StorageClass> classes;
  query << "SELECT NAME, NBCOPIES, UID, GID, CREATIONTIME, COMMENT FROM STORAGECLASS ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllStorageClasses() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    classes.push_back(cta::StorageClass(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            sqlite3_column_int(statement,cm("NBCOPIES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      ));
  }
  sqlite3_finalize(statement);  
  return classes;
}

//------------------------------------------------------------------------------
// selectAllArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute>  cta::SqliteDatabase::selectAllArchiveRoutes(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::ArchiveRoute> routes;
  query << "SELECT STORAGECLASS_NAME, COPYNB, TAPEPOOL_NAME, UID, GID, CREATIONTIME, COMMENT FROM ARCHIVEROUTE ORDER BY STORAGECLASS_NAME, COPYNB;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
    std::ostringstream message;
    message << "selectAllArchiveRoutes() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    routes.push_back(cta::ArchiveRoute(
            std::string((char *)sqlite3_column_text(statement,cm("STORAGECLASS_NAME"))),
            sqlite3_column_int(statement,cm("COPYNB")),
            std::string((char *)sqlite3_column_text(statement,cm("TAPEPOOL_NAME"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      ));
  }
  sqlite3_finalize(statement);
  return routes;
}

//------------------------------------------------------------------------------
// getArchiveRouteOfStorageClass
//------------------------------------------------------------------------------
cta::ArchiveRoute cta::SqliteDatabase::getArchiveRouteOfStorageClass(const SecurityIdentity &requester, const std::string &storageClassName, const  uint16_t copyNb) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "SELECT STORAGECLASS_NAME, COPYNB, TAPEPOOL_NAME, UID, GID, CREATIONTIME, COMMENT FROM ARCHIVEROUTE WHERE STORAGECLASS_NAME='"<< storageClassName <<"' AND COPYNB="<< (int)copyNb <<";";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "getArchiveRouteOfStorageClass() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  cta::ArchiveRoute route;
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    cm cm(statement);
    route = cta::ArchiveRoute(
            std::string((char *)sqlite3_column_text(statement,cm("STORAGECLASS_NAME"))),
            sqlite3_column_int(statement,cm("COPYNB")),
            std::string((char *)sqlite3_column_text(statement,cm("TAPEPOOL_NAME"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      );
  }
  else if(res==SQLITE_DONE) {
    std::ostringstream message;
    message << "getArchiveRouteOfStorageClass() - No archive route found for storage class: " << storageClassName << " and copynb: "<< (int)copyNb;
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  else {
    std::ostringstream message;
    message << "getArchiveRouteOfStorageClass() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  sqlite3_finalize(statement);
  return route;
}

//------------------------------------------------------------------------------
// selectAllTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::SqliteDatabase::selectAllTapes(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::Tape> tapes;
  query << "SELECT VID, LOGICALLIBRARY_NAME, TAPEPOOL_NAME, CAPACITY_BYTES, DATAONTAPE_BYTES, UID, GID, CREATIONTIME, COMMENT FROM TAPE ORDER BY VID;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllTapes() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    tapes.push_back(cta::Tape(
            std::string((char *)sqlite3_column_text(statement,cm("VID"))),
            std::string((char *)sqlite3_column_text(statement,cm("LOGICALLIBRARY_NAME"))),
            std::string((char *)sqlite3_column_text(statement,cm("TAPEPOOL_NAME"))),
            sqlite3_column_int(statement,cm("CAPACITY_BYTES")),
            sqlite3_column_int(statement,cm("DATAONTAPE_BYTES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),
            sqlite3_column_int(statement,cm("GID"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME")))
      ));
  }
  sqlite3_finalize(statement);
  return tapes;
}

//------------------------------------------------------------------------------
// selectAllAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::SqliteDatabase::selectAllAdminUsers(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::AdminUser> list;
  query << "SELECT ADMIN_UID, ADMIN_GID, UID, GID, CREATIONTIME, COMMENT FROM ADMINUSER ORDER BY ADMIN_UID, ADMIN_GID;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllAdminUsers() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    list.push_back(cta::AdminUser(
            cta::UserIdentity(sqlite3_column_int(statement,cm("ADMIN_UID")),sqlite3_column_int(statement,cm("ADMIN_GID"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      ));
  }
  sqlite3_finalize(statement);
  return list;
}

//------------------------------------------------------------------------------
// selectAllAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::SqliteDatabase::selectAllAdminHosts(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::AdminHost> list;
  query << "SELECT NAME, UID, GID, CREATIONTIME, COMMENT FROM ADMINHOST ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllAdminHosts() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    list.push_back(cta::AdminHost(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      ));
  }
  sqlite3_finalize(statement);
  return list;
}
  
//------------------------------------------------------------------------------
// selectAllArchivalJobs
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchivalJob> > cta::SqliteDatabase::selectAllArchivalJobs(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::map<cta::TapePool, std::list<cta::ArchivalJob> > map;
  query << "SELECT STATE, SRCURL, DSTPATH, TAPEPOOL_NAME, UID, GID, CREATIONTIME FROM ARCHIVALJOB ORDER BY DSTPATH;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllArchivalJobs() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    map[getTapePoolByName(requester, std::string((char *)sqlite3_column_text(statement,cm("TAPEPOOL_NAME"))))].push_back(cta::ArchivalJob(
            (cta::ArchivalJobState::Enum)sqlite3_column_int(statement,cm("STATE")),
            std::string((char *)sqlite3_column_text(statement,cm("SRCURL"))),
            std::string((char *)sqlite3_column_text(statement,cm("DSTPATH"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME")))
      ));
  }
  sqlite3_finalize(statement);
  return map;
}

//------------------------------------------------------------------------------
// getTapePoolByName
//------------------------------------------------------------------------------
cta::TapePool cta::SqliteDatabase::getTapePoolByName(const SecurityIdentity &requester, const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  cta::TapePool pool;
  query << "SELECT NAME, NBDRIVES, NBPARTIALTAPES, UID, GID, CREATIONTIME, COMMENT FROM TAPEPOOL WHERE NAME='" << name << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "getTapePoolByName() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    cm cm(statement);
    pool = cta::TapePool(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            sqlite3_column_int(statement,cm("NBDRIVES")),
            sqlite3_column_int(statement,cm("NBPARTIALTAPES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME")))
      );
  }
  else if(res==SQLITE_DONE) {    
    std::ostringstream message;
    message << "getTapePoolByName() - No tape pool found with name: " << name;
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  else {    
    std::ostringstream message;
    message << "getTapePoolByName() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  sqlite3_finalize(statement);
  return pool;
}

//------------------------------------------------------------------------------
// getStorageClassByName
//------------------------------------------------------------------------------
cta::StorageClass cta::SqliteDatabase::getStorageClassByName(const SecurityIdentity &requester, const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  cta::StorageClass stgClass;
  query << "SELECT NAME, NBCOPIES, UID, GID, CREATIONTIME, COMMENT FROM STORAGECLASS WHERE NAME='" << name << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "getStorageClassByName() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    cm cm(statement);
    stgClass = cta::StorageClass(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            sqlite3_column_int(statement,cm("NBCOPIES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      );
  }
  else if(res==SQLITE_DONE) {    
    std::ostringstream message;
    message << "getStorageClassByName() - No storage class found with name: " << name;
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  else {    
    std::ostringstream message;
    message << "getStorageClassByName() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  sqlite3_finalize(statement);
  return stgClass;
}

//------------------------------------------------------------------------------
// getTapeByVid
//------------------------------------------------------------------------------
cta::Tape cta::SqliteDatabase::getTapeByVid(const SecurityIdentity &requester, const std::string &vid) {
  char *zErrMsg = 0;
  std::ostringstream query;
  cta::Tape tape;
  query << "SELECT VID, LOGICALLIBRARY_NAME, TAPEPOOL_NAME, CAPACITY_BYTES, DATAONTAPE_BYTES, UID, GID, CREATIONTIME, COMMENT FROM TAPE WHERE VID='" << vid << "';";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){
    std::ostringstream message;
    message << "getTapeByVid() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  int res = sqlite3_step(statement);
  if(res==SQLITE_ROW) {
    cm cm(statement);
    tape = cta::Tape(
            std::string((char *)sqlite3_column_text(statement,cm("VID"))),
            std::string((char *)sqlite3_column_text(statement,cm("LOGICALLIBRARY_NAME"))),
            std::string((char *)sqlite3_column_text(statement,cm("TAPEPOOL_NAME"))),
            sqlite3_column_int(statement,cm("CAPACITY_BYTES")),
            sqlite3_column_int(statement,cm("DATAONTAPE_BYTES")),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),
            sqlite3_column_int(statement,cm("GID"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))))
      );
  }
  else if(res==SQLITE_DONE) {    
    std::ostringstream message;
    message << "getTapeByVid() - No tape found with vid: " << vid;
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  else {    
    std::ostringstream message;
    message << "getTapeByVid() - SQLite error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    sqlite3_finalize(statement);
    throw(Exception(message.str()));
  }
  sqlite3_finalize(statement);
  return tape;
}

//------------------------------------------------------------------------------
// selectAllRetrievalJobs
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrievalJob> > cta::SqliteDatabase::selectAllRetrievalJobs(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::map<cta::Tape, std::list<cta::RetrievalJob> > map;
  query << "SELECT STATE, SRCPATH, DSTURL, VID, UID, GID, CREATIONTIME FROM RETRIEVALJOB ORDER BY DSTURL;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllRetrievalJobs() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    map[getTapeByVid(requester, std::string((char *)sqlite3_column_text(statement,cm("VID"))))].push_back(cta::RetrievalJob(
            (cta::RetrievalJobState::Enum)sqlite3_column_int(statement,cm("STATE")),
            std::string((char *)sqlite3_column_text(statement,cm("SRCPATH"))),
            std::string((char *)sqlite3_column_text(statement,cm("DSTURL"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME")))
      ));
  }
  sqlite3_finalize(statement);
  return map;
}

//------------------------------------------------------------------------------
// selectAllLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::SqliteDatabase::selectAllLogicalLibraries(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::LogicalLibrary> list;
  query << "SELECT NAME, UID, GID, CREATIONTIME, COMMENT FROM LOGICALLIBRARY ORDER BY NAME;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "selectAllLogicalLibraries() - SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      sqlite3_finalize(statement);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    cm cm(statement);
    list.push_back(cta::LogicalLibrary(
            std::string((char *)sqlite3_column_text(statement,cm("NAME"))),
            cta::UserIdentity(sqlite3_column_int(statement,cm("UID")),sqlite3_column_int(statement,cm("GID"))),
            time_t(sqlite3_column_int(statement,cm("CREATIONTIME"))),
            std::string((char *)sqlite3_column_text(statement,cm("COMMENT")))
      ));
  }
  sqlite3_finalize(statement);
  return list;
}
