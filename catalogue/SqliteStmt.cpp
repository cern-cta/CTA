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

#include "catalogue/SqliteStmt.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteStmt::SqliteStmt(const SqliteConn &conn,
  const std::string &sql): m_sql(sql) {
  m_stmt = NULL;
  const int nByte = -1; // Read SQL up to first null terminator
  const int prepareRc = sqlite3_prepare_v2(conn.get(), sql.c_str(), nByte,
    &m_stmt, NULL);
  if(SQLITE_OK != prepareRc) {
    sqlite3_finalize(m_stmt);
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Failed to prepare SQL statement " << sql;
    throw ex;
  }
} 

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteStmt::~SqliteStmt() throw() {
  sqlite3_finalize(m_stmt);
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3_stmt *cta::catalogue::SqliteStmt::get() const {
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
void cta::catalogue::SqliteStmt::bind(const std::string &paramName,
  const uint64_t paramValue) {
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
void cta::catalogue::SqliteStmt::bind(const std::string &paramName,
  const std::string &paramValue) {
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
int cta::catalogue::SqliteStmt::step() {
  const int stepRc = sqlite3_step(m_stmt);

  // Return the result if sqlite_3_step was sucessful
  if(stepRc == SQLITE_DONE || stepRc == SQLITE_ROW) {
    return stepRc;
  }

  // Throw an appropriate exception
  exception::Exception ex;
  ex.getMessage() << __FUNCTION__ << " failed: " << sqlite3_errstr(stepRc) <<
    ": For SQL statement " << m_sql;
  throw ex;
}

//------------------------------------------------------------------------------
// getColumnNameToIdx
//------------------------------------------------------------------------------
cta::catalogue::ColumnNameToIdx cta::catalogue::SqliteStmt::getColumnNameToIdx()
  const {
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
    ex.getMessage() << __FUNCTION__ << " failed: For SQL statement " << m_sql
      << ": " << ne.getMessage();
    throw ex;
  }

  return nameToIdx;
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
std::string cta::catalogue::SqliteStmt::columnText(const int colIdx) {
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
uint64_t cta::catalogue::SqliteStmt::columnUint64(const int colIdx) {
  return (uint64_t)sqlite3_column_int64(m_stmt, colIdx);
} 

//------------------------------------------------------------------------------
// getParamIndex
//------------------------------------------------------------------------------
int cta::catalogue::SqliteStmt::getParamIndex(const std::string &paramName) {
  const int index = sqlite3_bind_parameter_index(m_stmt, paramName.c_str());
  if(0 == index) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Bind parameter " << paramName << " not found in SQL statement " <<
      m_sql;
    throw ex;
  }
  return index;
}
