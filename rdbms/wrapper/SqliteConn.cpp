/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/SqliteConn.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/Sqlite.hpp"
#include "rdbms/wrapper/SqliteStmt.hpp"

#include <iostream>
#include <stdexcept>
#include <string.h>
#include <string>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConn::SqliteConn(const rdbms::Login& login) : m_dbNamespace(login.dbNamespace) {
  m_sqliteConn = nullptr;
  if (sqlite3_open_v2(login.database.c_str(),
                      &m_sqliteConn,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                      nullptr)) {
    std::string msg = sqlite3_errmsg(m_sqliteConn);
    sqlite3_close(m_sqliteConn);
    throw exception::Exception(msg);
  }

  // Enable extended result codes
  sqlite3_extended_result_codes(m_sqliteConn, 1);

  // Give SQLite upto 120 seconds to avoid a busy error
  sqlite3_busy_timeout(m_sqliteConn, 120000);

  {
    char* errMsg = nullptr;
    if (SQLITE_OK != sqlite3_exec(m_sqliteConn, "PRAGMA foreign_keys = ON;", nullptr, nullptr, &errMsg)) {
      exception::Exception ex;
      ex.getMessage() << "Failed to to set PRAGMA foreign_keys = ON";
      if (nullptr != errMsg) {
        ex.getMessage() << ": " << errMsg;
        sqlite3_free(errMsg);
      }
      sqlite3_close(m_sqliteConn);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteConn::~SqliteConn() {
  try {
    close();  // Idempotent close() method
  } catch (...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteConn::close() {
  threading::MutexLocker locker(m_mutex);

  if (nullptr != m_sqliteConn) {
    if (const int closeRc = sqlite3_close(m_sqliteConn); SQLITE_OK != closeRc) {
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
  if (AutocommitMode::AUTOCOMMIT_OFF == autocommitMode) {
    throw rdbms::Conn::AutocommitModeNotSupported("Failed to set autocommit mode to AUTOCOMMIT_OFF: SqliteConn only"
                                                  " supports AUTOCOMMIT_ON");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode SqliteConn::getAutocommitMode() const noexcept {
  return AutocommitMode::AUTOCOMMIT_ON;
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteConn::executeNonQuery(const std::string& sql) {
  auto stmt = createStmt(sql);
  stmt->executeNonQuery();
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<StmtWrapper> SqliteConn::createStmt(const std::string& sql) {
  threading::MutexLocker locker(m_mutex);

  if (nullptr == m_sqliteConn) {
    throw exception::Exception("Connection is closed");
  }

  return std::make_unique<SqliteStmt>(*this, sql);
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void SqliteConn::commit() {
  threading::MutexLocker locker(m_mutex);

  if (nullptr == m_sqliteConn) {
    throw exception::Exception("Connection is closed");
  }

  char* errMsg = nullptr;
  if (SQLITE_OK != sqlite3_exec(m_sqliteConn, "COMMIT", nullptr, nullptr, &errMsg)) {
    if (nullptr == errMsg) {
      throw exception::Exception("sqlite3_exec failed");
    } else if (strcmp("cannot commit - no transaction is active", errMsg)) {
      exception::Exception ex;
      ex.getMessage() << "sqlite3_exec failed: " << errMsg;
      sqlite3_free(errMsg);
      throw ex;
    } else {
      sqlite3_free(errMsg);
    }
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void SqliteConn::rollback() {
  threading::MutexLocker locker(m_mutex);

  if (nullptr == m_sqliteConn) {
    throw exception::Exception("Connection is closed");
  }

  char* errMsg = nullptr;
  if (SQLITE_OK != sqlite3_exec(m_sqliteConn, "ROLLBACK", nullptr, nullptr, &errMsg)) {
    exception::Exception ex;
    ex.getMessage() << "sqlite3_exec failed";
    if (nullptr != errMsg) {
      ex.getMessage() << ": " << errMsg;
      sqlite3_free(errMsg);
    }
    throw ex;
  }
}

//------------------------------------------------------------------------------
// printSchema
//------------------------------------------------------------------------------
void SqliteConn::printSchema(std::ostream& os) {
  const char* const sql = R"SQL(
    SELECT
      NAME AS NAME,
      TYPE AS TYPE
    FROM
      SQLITE_MASTER
    ORDER BY
      TYPE,
      NAME
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  os << "NAME, TYPE" << std::endl;
  os << "==========" << std::endl;
  while (rset->next()) {
    const auto name = rset->columnOptionalString("NAME");
    const auto type = rset->columnOptionalString("TYPE");
    os << (name ? name.value() : "NULL") << ", " << (type ? type.value() : "NULL") << std::endl;
  }
}

//------------------------------------------------------------------------------
// getColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string, std::less<>> SqliteConn::getColumns(const std::string& tableName) {
  std::map<std::string, std::string, std::less<>> columnNamesAndTypes;
  const char* const sql = R"SQL(
    SELECT
      SQL AS SQL
    FROM
      (
        SELECT TBL_NAME, TYPE, SQL FROM SQLITE_MASTER
          UNION ALL
        SELECT TBL_NAME, TYPE, SQL FROM SQLITE_TEMP_MASTER
      )
    WHERE
      TBL_NAME = :TABLE_NAME
    AND
    TYPE = 'table'
  )SQL";
  const std::string columnTypes = "NUMERIC|"
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
  if (auto rset = stmt->executeQuery(); rset->next()) {
    auto tableSql = rset->columnOptionalString("SQL").value();
    tableSql += std::string(",");  // hack for parsing
    std::string::size_type searchPosComma = 0;
    std::string::size_type findResultComma = std::string::npos;
    while (std::string::npos != (findResultComma = tableSql.find(',', searchPosComma))) {
      const std::string::size_type stmtLenComma = findResultComma - searchPosComma;
      const std::string sqlStmtComma = utils::trimString(tableSql.substr(searchPosComma, stmtLenComma));
      searchPosComma = findResultComma + 1;
      if (0 < sqlStmtComma.size()) {  // Ignore empty statements
        const std::string columnSQL = "([a-zA-Z_0-9]+) +(" + columnTypes + ")";
        cta::utils::Regex columnSqlRegex(columnSQL);
        auto columnSql = columnSqlRegex.exec(sqlStmtComma);
        if (3 == columnSql.size()) {
          columnNamesAndTypes.insert(std::make_pair(columnSql[1], columnSql[2]));
        }
      }
    }
  }
  return columnNamesAndTypes;
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getTableNames() {
  const char* const sql = R"SQL(
    SELECT
      NAME AS NAME
    FROM
      (
        SELECT NAME, TYPE FROM SQLITE_MASTER
          UNION ALL
        SELECT NAME, TYPE FROM SQLITE_TEMP_MASTER
      )
    WHERE
      TYPE = 'table'
    ORDER BY
      NAME
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  std::vector<std::string> names;
  while (rset->next()) {
    auto name = rset->columnOptionalString("NAME");
    if (name) {
      names.push_back(name.value());
    }
  }
  return names;
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getIndexNames() {
  const char* const sql = R"SQL(
    SELECT
      NAME AS NAME
    FROM
      (
        SELECT NAME, TYPE FROM SQLITE_MASTER
          UNION ALL
        SELECT NAME, TYPE FROM SQLITE_TEMP_MASTER
      )
    WHERE
      TYPE = 'index'
    ORDER BY
      NAME
  )SQL";
  auto stmt = createStmt(sql);
  auto rset = stmt->executeQuery();
  std::vector<std::string> names;
  while (rset->next()) {
    auto name = rset->columnOptionalString("NAME");
    if (name) {
      names.push_back(name.value());
    }
  }
  return names;
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
std::vector<std::string> SqliteConn::getSequenceNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getTriggerNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getParallelTableNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getParallelTableNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getConstraintNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getConstraintNames(const std::string& tableName) {
  std::vector<std::string> constraintNames;
  const char* const sql = R"SQL(
    SELECT
      SQL AS SQL
    FROM
      (
        SELECT SQL, TYPE, NAME FROM SQLITE_MASTER
          UNION ALL
        SELECT SQL, TYPE, NAME FROM SQLITE_TEMP_MASTER
      )
    WHERE TYPE = 'table'
      AND NAME = :TABLE_NAME
  )SQL";
  auto stmt = createStmt(sql);
  stmt->bindString(":TABLE_NAME", tableName);
  if (auto rset = stmt->executeQuery(); rset->next()) {
    auto tableSql = rset->columnOptionalString("SQL").value();
    tableSql += std::string(",");  // hack for parsing
    std::string::size_type searchPosComma = 0;
    std::string::size_type findResultComma = std::string::npos;
    while (std::string::npos != (findResultComma = tableSql.find(',', searchPosComma))) {
      const std::string::size_type stmtLenComma = findResultComma - searchPosComma;
      const std::string sqlStmtComma = utils::trimString(tableSql.substr(searchPosComma, stmtLenComma));
      searchPosComma = findResultComma + 1;
      if (0 < sqlStmtComma.size()) {  // Ignore empty statements
        const std::string constraintSQL = "CONSTRAINT ([a-zA-Z_0-9]+)";
        cta::utils::Regex constraintSQLRegex(constraintSQL);
        auto constraintSql = constraintSQLRegex.exec(sqlStmtComma);
        if (2 == constraintSql.size()) {
          constraintNames.emplace_back(constraintSql[1]);
        }
      }
    }
  }
  return constraintNames;
}

//------------------------------------------------------------------------------
// getStoredProcedureNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getStoredProcedureNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getSynonymNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getSynonymNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getTypeNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getTypeNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getViewNames
//------------------------------------------------------------------------------
std::vector<std::string> SqliteConn::getViewNames() {
  return std::vector<std::string>();
}

//------------------------------------------------------------------------------
// getDbNamespace
//------------------------------------------------------------------------------
std::string SqliteConn::getDbNamespace() const {
  return m_dbNamespace;
}

}  // namespace cta::rdbms::wrapper
