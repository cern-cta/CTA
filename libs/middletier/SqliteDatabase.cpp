#include "Exception.hpp"
#include "SqliteDatabase.hpp"

#include <iostream>
#include <memory>
#include <sstream>

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
      message << "SQLite error: " << zErrMsg;
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
// createMigrationRouteTable
//------------------------------------------------------------------------------
void cta::SqliteDatabase::createMigrationRouteTable() {
  char *zErrMsg = 0;
  int rc = sqlite3_exec(m_dbHandle, 
          "CREATE TABLE MIGRATIONROUTE("
            "STORAGECLASS_NAME TEXT     PRIMARY KEY,"
            "COPYNB            INTEGER  PRIMARY KEY,"
            "TAPEPOOL_NAME     TEXT,"
            "UID               INTEGER,"
            "GID               INTEGER,"
            "COMMENT           TEXT,"
            "FOREIGN KEY(STORAGECLASS_NAME) REFERENCES STORAGECLASS(NAME),"
            "FOREIGN KEY(TAPEPOOL_NAME)     REFERENCES TAPEPOOL(NAME)"
            ");",
          0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// createStorageClassTable
//------------------------------------------------------------------------------
void cta::SqliteDatabase::createStorageClassTable() {
  char *zErrMsg = 0;
  int rc = sqlite3_exec(m_dbHandle, 
          "CREATE TABLE STORAGECLASS("
            "NAME           TEXT     PRIMARY KEY,"
            "NBCOPIES       INTEGER,"
            "UID            INTEGER,"
            "GID            INTEGER,"
            "COMMENT        TEXT"
            ");",
          0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// createTapePoolTable
//------------------------------------------------------------------------------
void cta::SqliteDatabase::createTapePoolTable() {
  char *zErrMsg = 0;
  int rc = sqlite3_exec(m_dbHandle, 
          "CREATE TABLE TAPEPOOL("
            "NAME           TEXT     PRIMARY KEY,"
            "NBDRIVES       INTEGER,"
            "NBPARTIALTAPES INTEGER,"
            "UID            INTEGER,"
            "GID            INTEGER,"
            "COMMENT        TEXT"
            ");",
          0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// createSchema
//------------------------------------------------------------------------------
void cta::SqliteDatabase::createSchema() {
  createStorageClassTable();
  createTapePoolTable();
  createMigrationRouteTable();
}

//------------------------------------------------------------------------------
// insertTapePool
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertTapePool(const SecurityIdentity &requester, const std::string &name, const uint16_t nbDrives, const uint32_t nbPartialTapes, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO TAPEPOOL VALUES('" << name << "'," << nbDrives << "," << nbPartialTapes << "," << requester.user.getUid() << "," << requester.user.getGid() << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// insertStorageClass
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertStorageClass(const SecurityIdentity &requester, const std::string &name, const uint8_t nbCopies, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO STORAGECLASS VALUES('" << name << "'," << nbCopies << "," << requester.user.getUid() << "," << requester.user.getGid() << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// insertMigrationRoute
//------------------------------------------------------------------------------
void cta::SqliteDatabase::insertMigrationRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb, const std::string &tapePoolName, const std::string &comment) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "INSERT INTO MIGRATIONROUTE VALUES('" << storageClassName << "'," << copyNb << ",'" << tapePoolName << "'," << requester.user.getUid() << "," << requester.user.getGid() << ",'" << comment << "');";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteTapePool(const SecurityIdentity &requester, const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM TAPEPOOL WHERE NAME='" << name << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteStorageClass(const SecurityIdentity &requester, const std::string &name) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM STORAGECLASS WHERE NAME='" << name << "';";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteMigrationRoute
//------------------------------------------------------------------------------
void cta::SqliteDatabase::deleteMigrationRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb) {
  char *zErrMsg = 0;
  std::ostringstream query;
  query << "DELETE FROM MIGRATIONROUTE WHERE NAME='" << storageClassName << "' AND COPYNB=" << copyNb << ";";
  int rc = sqlite3_exec(m_dbHandle, query.str().c_str(), 0, 0, &zErrMsg);
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// selectAllTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::SqliteDatabase::selectAllTapePools(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::TapePool> pools;
  query << "SELECT * FROM TAPEPOOL;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    pools.push_back(cta::TapePool(
            std::string((char *)sqlite3_column_text(statement,0)),
            sqlite3_column_int(statement,1),
            sqlite3_column_int(statement,2),
            cta::UserIdentity(sqlite3_column_int(statement,3),sqlite3_column_int(statement,4)),
            std::string((char *)sqlite3_column_text(statement,5))
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
  query << "SELECT * FROM STORAGECLASS;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    classes.push_back(cta::StorageClass(
            std::string((char *)sqlite3_column_text(statement,0)),
            sqlite3_column_int(statement,1),
            cta::UserIdentity(sqlite3_column_int(statement,2),sqlite3_column_int(statement,3)),
            std::string((char *)sqlite3_column_text(statement,4))
      ));
  }
  sqlite3_finalize(statement);  
  return classes;
}

//------------------------------------------------------------------------------
// selectAllMigrationRoutes
//------------------------------------------------------------------------------
std::list<cta::MigrationRoute>  cta::SqliteDatabase::selectAllMigrationRoutes(const SecurityIdentity &requester) {
  char *zErrMsg = 0;
  std::ostringstream query;
  std::list<cta::MigrationRoute> routes;
  query << "SELECT * FROM MIGRATIONROUTE;";
  sqlite3_stmt *statement;
  int rc = sqlite3_prepare(m_dbHandle, query.str().c_str(), -1, &statement, 0 );
  if(rc!=SQLITE_OK){    
      std::ostringstream message;
      message << "SQLite error: " << zErrMsg;
      sqlite3_free(zErrMsg);
      throw(Exception(message.str()));
  }
  while(sqlite3_step(statement)==SQLITE_ROW) {
    routes.push_back(cta::MigrationRoute(
            std::string((char *)sqlite3_column_text(statement,0)),
            sqlite3_column_int(statement,1),
            std::string((char *)sqlite3_column_text(statement,2)),
            cta::UserIdentity(sqlite3_column_int(statement,3),sqlite3_column_int(statement,4)),
            std::string((char *)sqlite3_column_text(statement,5))
      ));
  }
  sqlite3_finalize(statement);
  return routes;
}