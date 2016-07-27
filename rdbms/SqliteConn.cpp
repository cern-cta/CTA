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
#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "rdbms/SqliteConn.hpp"
#include "rdbms/SqliteStmt.hpp"

#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConn::SqliteConn(const std::string &filename) {
  try {
    m_conn = nullptr;
    if(sqlite3_open_v2(filename.c_str(), &m_conn, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI, nullptr)) {
      std::string msg = sqlite3_errmsg(m_conn);
      sqlite3_close(m_conn);
      throw exception::Exception(msg);
    }
    {
      char *errMsg = nullptr;
      if(SQLITE_OK != sqlite3_exec(m_conn, "PRAGMA foreign_keys = ON;", nullptr, nullptr, &errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "Failed to to set PRAGMA foreign_keys = ON";
        if(nullptr != errMsg) {
          ex.getMessage() << ": " << errMsg;
          sqlite3_free(errMsg);
        }
        sqlite3_close(m_conn);
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
SqliteConn::~SqliteConn() throw() {
  close();
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteConn::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(m_conn != nullptr) {
    sqlite3_close(m_conn);
    m_conn = nullptr;
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<Stmt> SqliteConn::createStmt(const std::string &sql) {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    sqlite3_stmt *stmt = nullptr;
    const int nByte = -1; // Read SQL up to first null terminator
    const int prepareRc = sqlite3_prepare_v2(m_conn, sql.c_str(), nByte, &stmt, nullptr);
    if (SQLITE_OK != prepareRc) {
      const std::string msg = sqlite3_errmsg(m_conn);
      sqlite3_finalize(stmt);
      throw exception::Exception(msg);
    }

    return make_unique<SqliteStmt>(*this, sql, stmt);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " +
      ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void SqliteConn::commit() {
  try {
    executeNonQuery("COMMIT;");
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void SqliteConn::rollback() {
  try {
    executeNonQuery("ROLLBACK;");
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
    std::unique_ptr<Stmt> stmt(createStmt(sql));
    std::unique_ptr<Rset> rset(stmt->executeQuery());
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

} // namespace rdbms
} // namespace cta
