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
cta::catalogue::SqliteStmt::SqliteStmt(SqliteConn &conn,
  const std::string &sql): m_sql(sql) {
  m_stmt = NULL;
  const int prepareRc = sqlite3_prepare_v2(conn.get(), sql.c_str(), 0, &m_stmt,
    NULL);
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
  const int bindRc = sqlite3_bind_text(m_stmt, paramIdx, paramName.c_str(), -1,
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
  ex.getMessage() << __FUNCTION__ << " failed: " << sqliteRcToString(stepRc) <<
    ": For SQL statement " << m_sql;
  throw ex;
}

//------------------------------------------------------------------------------
// sqliteRcToString
//------------------------------------------------------------------------------
std::string cta::catalogue::SqliteStmt::sqliteRcToString(const int rc) const {
  switch(rc) {
  case SQLITE_ABORT:
    return "Abort requested";
  case SQLITE_AUTH:
    return "Authorization denied";
  case SQLITE_BUSY:
    return "Failed to take locks";
  case SQLITE_CANTOPEN:
    return "Cannot open database file";
  case SQLITE_CONSTRAINT:
    return "Constraint violation";
  case SQLITE_CORRUPT:
    return "Database file corrupted";
  case SQLITE_DONE:
    return "Statement finished executing successfully";
  case SQLITE_EMPTY:
    return "Database file empty";
  case SQLITE_FORMAT:
    return "Database format error";
  case SQLITE_FULL:
    return "Database full";
  case SQLITE_INTERNAL:
    return "Internal SQLite library error";
  case SQLITE_INTERRUPT:
    return "Interrupted";
  case SQLITE_IOERR:
    return "I/O error";
  case SQLITE_LOCKED:
    return "A table is locked";
  case SQLITE_MISMATCH:
    return "Datatype mismatch";
  case SQLITE_MISUSE:
    return "Misuse";
  case SQLITE_NOLFS:
    return "OS cannot provide functionality";
  case SQLITE_NOMEM:
    return "Memory allocation error";
  case SQLITE_NOTADB:
    return "Not a database file";
  case SQLITE_OK:
    return "Operation successful";
  case SQLITE_PERM:
    return "Permnission denied";
  case SQLITE_RANGE:
    return "Invalid bind parameter index";
  case SQLITE_READONLY:
    return "Failed to write to read-only database";
  case SQLITE_ROW:
    return "A new row of data is ready for reading";
  case SQLITE_SCHEMA:
    return "Database schema changed";
  case SQLITE_TOOBIG:
    return "TEXT or BLOCK too big";
  case SQLITE_ERROR:
    return "Generic error";
  default:
    {
      std::ostringstream oss;
      oss << "Unknown SQLite return code " << rc;
      return oss.str();
    }
  }
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
