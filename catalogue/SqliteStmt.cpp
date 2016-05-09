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

#include "catalogue/Sqlite.hpp"
#include "catalogue/SqliteConn.hpp"
#include "catalogue/SqliteRset.hpp"
#include "catalogue/SqliteStmt.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteStmt::SqliteStmt(const char *const sql, sqlite3_stmt *const stmt):
  m_stmt(stmt) {
  if (NULL == sql) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: sql is NULL");
  }

  if (NULL == stmt) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": stmt is NULL");
  }

  // Work with C strings because they haven't changed with respect to CXX11 ABI
  const std::size_t sqlLen = std::strlen(sql);
  m_sql.reset(new char[sqlLen + 1]);
  std::memcpy(m_sql.get(), sql, sqlLen);
  m_sql[sqlLen] = '\0';
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

  if(NULL != m_stmt) {
    sqlite3_finalize(m_stmt);
    m_stmt = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3_stmt *SqliteStmt::get() const {
  if(NULL == m_stmt) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() +
      ": NULL pointer");
  }
  return m_stmt;
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const char *SqliteStmt::getSql() const {
  return m_sql.get();
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bind(const char *const paramName, const uint64_t paramValue) {
  const int paramIdx = getParamIndex(paramName);
  const int bindRc = sqlite3_bind_int64(m_stmt, paramIdx, (sqlite3_int64)paramValue);
  if(SQLITE_OK != bindRc) {
    throw std::runtime_error(std::string(__FUNCTION__) + "failed for SQL statement " + getSql());
  }
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bind(const char *const paramName, const std::string &paramValue) {
  const int paramIdx = getParamIndex(paramName);
  const int bindRc = sqlite3_bind_text(m_stmt, paramIdx, paramValue.c_str(), -1,
    SQLITE_TRANSIENT);
  if(SQLITE_OK != bindRc) {
    throw std::runtime_error(std::string(__FUNCTION__) + "failed for SQL statement " + getSql());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
SqliteRset *SqliteStmt::executeQuery() {
  return new SqliteRset(*this);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteStmt::executeNonQuery() {
  const int stepRc = sqlite3_step(m_stmt);

  // Throw an exception if the call to sqlite3_step() failed
  if(SQLITE_DONE != stepRc && SQLITE_ROW != stepRc) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() + ": " +
      Sqlite::rcToStr(stepRc));
  }

  // Throw an exception if the SQL statement returned a result set
  if(SQLITE_ROW == stepRc) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() +
      ": The SQL statment returned a result set");
  }
}

//------------------------------------------------------------------------------
// getParamIndex
//------------------------------------------------------------------------------
int SqliteStmt::getParamIndex(const char *const paramName) const {
  const int index = sqlite3_bind_parameter_index(m_stmt, paramName);
  if(0 == index) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed for SQL statement " + getSql() + ": " +
      ": Bind parameter " + paramName + " not found");
  }
  return index;
}

} // namespace catalogue
} // namespace cta
