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

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConn::SqliteConn(const std::string &filename) {
  m_conn = NULL;
  if(sqlite3_open(filename.c_str(), &m_conn)) {
    sqlite3_close(m_conn);
    exception::Exception ex;
    ex.getMessage() << "Failed to construct SqliteConn: sqlite3_open failed"
      ": " << sqlite3_errmsg(m_conn);
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteConn::~SqliteConn() throw() {
  close();
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteConn::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(m_conn != NULL) {
    sqlite3_close(m_conn);
    m_conn = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3 *SqliteConn::get() const {
  if(NULL == m_conn) {
    throw exception::Exception("Failed to get SQLite database connection"
      ": NULL pointer");
  }
  return m_conn;
}

//------------------------------------------------------------------------------
// enableForeignKeys
//------------------------------------------------------------------------------
void SqliteConn::enableForeignKeys() {
  try {
    execNonQuery("PRAGMA foreign_keys = ON;");
  } catch(exception::Exception &ne) {
    exception::Exception ex;
    ex.getMessage() << "Failed to enable foreign key constraints: " <<
       ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteConn::execNonQuery(const std::string &sql) {
  int (*callback)(void*,int,char**,char**) = NULL;
  void *callbackArg = NULL;
  char *errMsg = NULL;
  if(SQLITE_OK != sqlite3_exec(m_conn, sql.c_str(), callback, callbackArg,
    &errMsg)) {
    exception::Exception ex;
    ex.getMessage() << "Failed to execute non-query: " << sql << ": " << errMsg;
    sqlite3_free(errMsg);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
SqliteStmt *SqliteConn::createStmt(
  const std::string &sql) {
  std::lock_guard<std::mutex> lock(m_mutex);

  sqlite3_stmt *stmt = NULL;
  const int nByte = -1; // Read SQL up to first null terminator
  const int prepareRc = sqlite3_prepare_v2(m_conn, sql.c_str(), nByte, &stmt,
    NULL);
  if(SQLITE_OK != prepareRc) {
    sqlite3_finalize(stmt);
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Failed to prepare SQL statement " << sql << ": " <<
      sqlite3_errmsg(m_conn);
    throw ex;
  }

  return new SqliteStmt(sql, stmt);
}

} // namespace catalogue
} // namespace cta
