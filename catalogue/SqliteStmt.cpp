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
#include "catalogue/SqliteStmt.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteStmt::SqliteStmt(const std::string &sql, sqlite3_stmt *const stmt):
  m_sql(sql),
  m_stmt(stmt) {
  if(NULL == stmt) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": The prepared statement is a NULL pointer";
    throw ex;
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

  if(NULL != m_stmt) {
    sqlite3_finalize(m_stmt);
    m_stmt = NULL;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &SqliteStmt::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3_stmt *SqliteStmt::get() const {
  if(NULL == m_stmt) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: NULL pointer";
    throw ex;
  }
  return m_stmt;
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bind(const std::string &paramName, const uint64_t paramValue) {
  const int paramIdx = getParamIndex(paramName);
  const int bindRc = sqlite3_bind_int64(m_stmt, paramIdx,
    (sqlite3_int64)paramValue);
  if(SQLITE_OK != bindRc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void SqliteStmt::bind(const std::string &paramName, const std::string &paramValue) {
  const int paramIdx = getParamIndex(paramName);
  const int bindRc = sqlite3_bind_text(m_stmt, paramIdx, paramValue.c_str(), -1,
    SQLITE_TRANSIENT);
  if(SQLITE_OK != bindRc) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// step
//------------------------------------------------------------------------------
int SqliteStmt::step() {
  const int stepRc = sqlite3_step(m_stmt);

  // Return the result if sqlite_3_step was sucessful
  if(stepRc == SQLITE_DONE || stepRc == SQLITE_ROW) {
    return stepRc;
  }

  // Throw an appropriate exception
  exception::Exception ex;
  ex.getMessage() << __FUNCTION__ << " failed: " << Sqlite::rcToStr(stepRc) <<
    ": For SQL statement " << utils::singleSpaceString(m_sql);
  throw ex;
}

//------------------------------------------------------------------------------
// getColumnNameToIdx
//------------------------------------------------------------------------------
ColumnNameToIdx cta::catalogue::SqliteStmt::getColumnNameToIdx() const {
  ColumnNameToIdx nameToIdx;

  try {
    const int nbCols = sqlite3_column_count(m_stmt);
    for(int i = 0; i < nbCols; i++) {
      const char *name = sqlite3_column_name(m_stmt, i);
      if(NULL == name) {
        exception::Exception ex;
        ex.getMessage() << "Failed to get column name for column index " << i;
        throw ex;
      }
      nameToIdx.add(name, i);
    }
  } catch(exception::Exception &ne) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed: For SQL statement " << utils::singleSpaceString(m_sql)
      << ": " << ne.getMessage().str();
    throw ex;
  }

  return nameToIdx;
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
std::string SqliteStmt::columnText(const int colIdx) {
  const char *const colValue = (const char *)sqlite3_column_text(m_stmt,
    colIdx);
  if(NULL == colValue) {
    return "";
  } else  {
    return colValue;
  }
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t SqliteStmt::columnUint64(const int colIdx) {
  return (uint64_t)sqlite3_column_int64(m_stmt, colIdx);
} 

//------------------------------------------------------------------------------
// getParamIndex
//------------------------------------------------------------------------------
int SqliteStmt::getParamIndex(const std::string &paramName) {
  const int index = sqlite3_bind_parameter_index(m_stmt, paramName.c_str());
  if(0 == index) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Bind parameter " << paramName << " not found in SQL statement " <<
      utils::singleSpaceString(m_sql);
    throw ex;
  }
  return index;
}

} // namespace catalogue
} // namespace cta
