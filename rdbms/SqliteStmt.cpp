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

#include "Sqlite.hpp"
#include "SqliteConn.hpp"
#include "SqliteRset.hpp"
#include "SqliteStmt.hpp"
#include "common/exception/Exception.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteStmt::SqliteStmt(SqliteConn &conn, const std::string &sql, sqlite3_stmt *const stmt):
  m_conn(conn),
  m_sql(sql),
  m_paramNameToIdx(sql),
  m_stmt(stmt),
  m_nbAffectedRows(0) {

  if (nullptr == stmt) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": stmt is nullptr");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteStmt::~SqliteStmt() throw() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteStmt::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(nullptr != m_stmt) {
    sqlite3_finalize(m_stmt);
    m_stmt = nullptr;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3_stmt *SqliteStmt::get() const {
  if(nullptr == m_stmt) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() +
      ": nullptr pointer");
  }
  return m_stmt;
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &SqliteStmt::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bindUint64(const std::string &paramName, const uint64_t paramValue) {
  const unsigned int paramIdx = m_paramNameToIdx.getIdx(paramName);
  const int bindRc = sqlite3_bind_int64(m_stmt, paramIdx, (sqlite3_int64)paramValue);
  if(SQLITE_OK != bindRc) {
    throw exception::Exception(std::string(__FUNCTION__) + "failed for SQL statement " + getSql());
  }
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bindString(const std::string &paramName, const std::string &paramValue) {
  const unsigned int paramIdx = m_paramNameToIdx.getIdx(paramName);
  const int bindRc = paramValue.empty() ?
    sqlite3_bind_text(m_stmt, paramIdx, nullptr, 0, SQLITE_TRANSIENT) :
    sqlite3_bind_text(m_stmt, paramIdx, paramValue.c_str(), -1, SQLITE_TRANSIENT);
  if(SQLITE_OK != bindRc) {
    throw exception::Exception(std::string(__FUNCTION__) + "failed for SQL statement " + getSql());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
Rset *SqliteStmt::executeQuery() {
  return new SqliteRset(*this);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteStmt::executeNonQuery() {
  std::lock_guard<std::mutex> connLock(m_conn.m_mutex);

  const int stepRc = sqlite3_step(m_stmt);

  // Throw an exception if the call to sqlite3_step() failed
  if(SQLITE_DONE != stepRc && SQLITE_ROW != stepRc) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() + ": " +
      Sqlite::rcToStr(stepRc));
  }

  m_nbAffectedRows = sqlite3_changes(m_conn.m_conn);

  // Throw an exception if the SQL statement returned a result set
  if(SQLITE_ROW == stepRc) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() +
      ": The SQL statment returned a result set");
  }
}

//------------------------------------------------------------------------------
// getNbAffectedRows
//------------------------------------------------------------------------------
uint64_t SqliteStmt::getNbAffectedRows() const {
  return m_nbAffectedRows;
}

} // namespace rdbms
} // namespace cta
