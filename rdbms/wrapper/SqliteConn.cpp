/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"
#include "common/utils/Regex.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/Sqlite.hpp"
#include "rdbms/wrapper/SqliteConn.hpp"
#include "rdbms/wrapper/SqliteStmt.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <string.h>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConn::SqliteConn(const std::string &filename) {
  try {
    m_sqliteConn = nullptr;
    if(sqlite3_open_v2(filename.c_str(), &m_sqliteConn, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI, nullptr)) {
      std::string msg = sqlite3_errmsg(m_sqliteConn);
      sqlite3_close(m_sqliteConn);
      throw exception::Exception(msg);
    }

    // Enable extended result codes
    sqlite3_extended_result_codes(m_sqliteConn, 1);

    // Give SQLite upto 120 seconds to avoid a busy error
    sqlite3_busy_timeout(m_sqliteConn, 120000);

    {
      char *errMsg = nullptr;
      if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "PRAGMA foreign_keys = ON;", nullptr, nullptr, &errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "Failed to to set PRAGMA foreign_keys = ON";
        if(nullptr != errMsg) {
          ex.getMessage() << ": " << errMsg;
          sqlite3_free(errMsg);
        }
        sqlite3_close(m_sqliteConn);
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteConn::~SqliteConn() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteConn::close() {
  threading::MutexLocker locker(m_mutex);

  if(nullptr != m_sqliteConn) {
    const int closeRc = sqlite3_close(m_sqliteConn);
    if(SQLITE_OK != closeRc) {
      exception::Exception ex;
      ex.getMessage() << "Failed to close SQLite connection: " << Sqlite::rcToStr(closeRc);
      throw ex;
    }
    m_sqliteConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void SqliteConn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if(AutocommitMode::AUTOCOMMIT_OFF == autocommitMode) {
    throw rdbms::Conn::AutocommitModeNotSupported("Failed to set autocommit mode to AUTOCOMMIT_OFF: SqliteConn only"
      " supports AUTOCOMMIT_ON");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode SqliteConn::getAutocommitMode() const noexcept{
  return AutocommitMode::AUTOCOMMIT_ON;
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteConn::executeNonQuery(const std::string &sql) {
  try {
    auto stmt = createStmt(sql);
    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> SqliteConn::createStmt(const std::string &sql) {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    return cta::make_unique<SqliteStmt>(*this, sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void SqliteConn::commit() {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    char *errMsg = nullptr;
    if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "COMMIT", nullptr, nullptr, &errMsg)) {
      if(nullptr == errMsg) {
        throw exception::Exception("sqlite3_exec failed");
      } else if(strcmp("cannot commit - no transaction is active", errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "sqlite3_exec failed: " << errMsg;
        sqlite3_free(errMsg);
        throw ex;
      } else {
        sqlite3_free(errMsg);
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void SqliteConn::rollback() {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    char *errMsg = nullptr;
    if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "ROLLBACK", nullptr, nullptr, &errMsg)) {
      exception::Exception ex;
      ex.getMessage() << "sqlite3_exec failed";
      if(nullptr != errMsg) {
        ex.getMessage() << ": " << errMsg;
        sqlite3_free(errMsg);
      }
      throw ex;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// printSchema
//------------------------------------------------------------------------------
void SqliteConn::printSchema(std::ostream &os) {
  try {
    const char *const sql =
      "SELECT "
        "NAME AS NAME, "
        "TYPE AS TYPE "
      "FROM "
        "SQLITE_MASTER "
      "ORDER BY "
        "TYPE, "
        "NAME;";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    os << "NAME, TYPE" << std::endl;
    os << "==========" << std::endl;
    while (rset->next()) {
      const auto name = rset->columnOptionalString("NAME");
      const auto type = rset->columnOptionalString("TYPE");
      os << (name ? name.value() : "NULL") << ", " << (type ? type.value() : "NULL") << std::endl;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> SqliteConn::getColumns(const std::string &tableName) {
  try {
    std::map<std::string, std::string> columnNamesAndTypes;
    const char *const sql =
      "SELECT "
        "SQL AS SQL "
      "FROM "
        "("
          "SELECT TBL_NAME, TYPE, SQL FROM SQLITE_MASTER "
            "UNION ALL "
          "SELECT TBL_NAME, TYPE, SQL FROM SQLITE_TEMP_MASTER"
        ") "
      "WHERE "
        "TBL_NAME = :TABLE_NAME "
      "AND "
      "TYPE = 'table';";
    const std::string columnTypes = 
    "NUMERIC|"
    "INTEGER|"
    "CHAR|"
    "VARCHAR|"
    "VARCHAR2|"
    "BLOB|"
    "BIGINT|"
    "SMALLINT|"
    "INT|"
    "TINYINT|"
    "VARBINARY|"
    "BYTEA|"
    "RAW";
    
    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME", tableName);
    auto rset = stmt->executeQuery();
    if (rset->next()) {
      auto tableSql = rset->columnOptionalString("SQL").value();     
      tableSql += std::string(","); // hack for parsing          
      std::string::size_type searchPosComma = 0;
      std::string::size_type findResultComma = std::string::npos;
      while(std::string::npos != (findResultComma = tableSql.find(',', searchPosComma))) {
        const std::string::size_type stmtLenComma = findResultComma - searchPosComma;
        const std::string sqlStmtComma = utils::trimString(tableSql.substr(searchPosComma, stmtLenComma));
        searchPosComma = findResultComma + 1;
        if(0 < sqlStmtComma.size()) { // Ignore empty statements
          const std::string columnSQL = "([a-zA-Z_0-9]+) +(" + columnTypes + ")";
          cta::utils::Regex columnSqlRegex(columnSQL.c_str());
          auto columnSql = columnSqlRegex.exec(sqlStmtComma);
          if (3 == columnSql.size()) {
            columnNamesAndTypes.insert(std::make_pair(columnSql[1], columnSql[2]));
          }
        }
      }     
    }
    return columnNamesAndTypes;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } 
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getTableNames() {
  try {
    const char *const sql =
      "SELECT "
        "NAME AS NAME "
      "FROM "
        "("
          "SELECT NAME, TYPE FROM SQLITE_MASTER "
            "UNION ALL "
          "SELECT NAME, TYPE FROM SQLITE_TEMP_MASTER"
        ") "
      "WHERE "
        "TYPE = 'table' "
      "ORDER BY "
        "NAME;";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    std::list<std::string> names;
    while (rset->next()) {
      auto name = rset->columnOptionalString("NAME");
      if(name) {
        names.push_back(name.value());
      }
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getIndexNames() {
  try {
    const char *const sql =
      "SELECT "
        "NAME AS NAME "
      "FROM "
      "("
        "SELECT NAME, TYPE FROM SQLITE_MASTER "
          "UNION ALL "
        "SELECT NAME, TYPE FROM SQLITE_TEMP_MASTER"
      ") "
      "WHERE "
        "TYPE = 'index' "
      "ORDER BY "
        "NAME;";
    auto stmt = createStmt(sql);
    auto rset = stmt->executeQuery();
    std::list<std::string> names;
    while (rset->next()) {
      auto name = rset->columnOptionalString("NAME");
      if(name) {
        names.push_back(name.value());
      }
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool SqliteConn::isOpen() const {
  return nullptr != m_sqliteConn;
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getSequenceNames() {
  try {
    return std::list<std::string>();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getTriggerNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getParallelTableNames(){
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getConstraintNames(const std::string &tableName){
  try {
    std::list<std::string> constraintNames;
    const char *const sql = 
    "SELECT "
      "SQL AS SQL "
    "FROM "
      "("
        "SELECT SQL, TYPE, NAME FROM SQLITE_MASTER "
          "UNION ALL "
        "SELECT SQL, TYPE, NAME FROM SQLITE_TEMP_MASTER"
      ") "
    "WHERE TYPE = 'table' "
      "AND NAME = :TABLE_NAME ";
    auto stmt = createStmt(sql);
    stmt->bindString(":TABLE_NAME", tableName);
    auto rset = stmt->executeQuery();
    if (rset->next()) {
      auto tableSql = rset->columnOptionalString("SQL").value();     
      tableSql += std::string(","); // hack for parsing          
      std::string::size_type searchPosComma = 0;
      std::string::size_type findResultComma = std::string::npos;
      while(std::string::npos != (findResultComma = tableSql.find(',', searchPosComma))) {
        const std::string::size_type stmtLenComma = findResultComma - searchPosComma;
        const std::string sqlStmtComma = utils::trimString(tableSql.substr(searchPosComma, stmtLenComma));
        searchPosComma = findResultComma + 1;
        if(0 < sqlStmtComma.size()) { // Ignore empty statements
          const std::string constraintSQL = "CONSTRAINT ([a-zA-Z_0-9]+)";
          cta::utils::Regex constraintSQLRegex(constraintSQL.c_str());
          auto constraintSql = constraintSQLRegex.exec(sqlStmtComma);
          if (2 == constraintSql.size()) {
            constraintNames.emplace_back(constraintSql[1]);
          }
        }
      }     
    }
    return constraintNames;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
      }


//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getStoredProcedureNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getSynonymNames() {
  return std::list<std::string>();
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getTypeNames() {
  return std::list<std::string>();
}


} // namespace wrapper
} // namespace rdbms
} // namespace cta
