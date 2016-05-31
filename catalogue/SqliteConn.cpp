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

#include "catalogue/RdbmsCatalogueSchema.hpp"
#include "catalogue/SqliteConn.hpp"
#include "catalogue/SqliteStmt.hpp"
#include "common/exception/Exception.hpp"

#include <stdexcept>
#include <string>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConn::SqliteConn(const std::string &filename) {
  try {
    m_conn = NULL;
    if (sqlite3_open(filename.c_str(), &m_conn)) {
      sqlite3_close(m_conn);
      throw exception::Exception(
        std::string(__FUNCTION__) + "sqlite3_open() failed: " + sqlite3_errmsg(m_conn));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
// createStmt
//------------------------------------------------------------------------------
DbStmt *SqliteConn::createStmt(const std::string &sql) {
  std::lock_guard<std::mutex> lock(m_mutex);

  sqlite3_stmt *stmt = NULL;
  const int nByte = -1; // Read SQL up to first null terminator
  const int prepareRc = sqlite3_prepare_v2(m_conn, sql.c_str(), nByte, &stmt, NULL);
  if(SQLITE_OK != prepareRc) {
    sqlite3_finalize(stmt);
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql +
      ": " + sqlite3_errmsg(m_conn));
  }

  return new SqliteStmt(sql, stmt);
}

//------------------------------------------------------------------------------
// createCatalogueDatabaseSchema
//------------------------------------------------------------------------------
void SqliteConn::createCatalogueDatabaseSchema() {
  try {
    execMultiLineNonQuery("PRAGMA foreign_keys = ON;");
    const RdbmsCatalogueSchema schema;
    execMultiLineNonQuery(schema.sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeMultiLineNonQuery
//------------------------------------------------------------------------------
void SqliteConn::execMultiLineNonQuery(const std::string &sql) {
  int (*callback)(void*,int,char**,char**) = NULL;
  void *callbackArg = NULL;
  char *sqliteErrMsg = NULL;
  if(SQLITE_OK != sqlite3_exec(m_conn, sql.c_str(), callback, callbackArg, &sqliteErrMsg)) {
    std::string msg = std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " + sqliteErrMsg;
    sqlite3_free(sqliteErrMsg);
    throw exception::Exception(msg);
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
    std::unique_ptr<DbStmt> stmt(createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    os << "NAME, TYPE" << std::endl;
    os << "==========" << std::endl;
    while (rset->next()) {
      const std::string name = rset->columnText("NAME");
      const std::string type = rset->columnText("TYPE");
      os << name << ", " << type << std::endl;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
