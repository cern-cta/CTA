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

#include "catalogue/SqliteConn.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteConn::SqliteConn(const std::string &filename) {
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
cta::catalogue::SqliteConn::~SqliteConn() throw() {
  sqlite3_close(m_conn);
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3 *cta::catalogue::SqliteConn::get() const {
  if(NULL == m_conn) {
    throw exception::Exception("Failed to get SQLite database connection"
      ": NULL pointer");
  }
  return m_conn;
}

//------------------------------------------------------------------------------
// enableForeignKeys
//------------------------------------------------------------------------------
void cta::catalogue::SqliteConn::enableForeignKeys() {
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
void cta::catalogue::SqliteConn::execNonQuery(const std::string &sql) {
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
