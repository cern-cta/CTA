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
SqliteConn::SqliteConn(const std::string &filename):
  m_transactionInProgress(false) {
  try {
    m_sqliteConn = nullptr;
    if(sqlite3_open_v2(filename.c_str(), &m_sqliteConn, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI, nullptr)) {
      std::string msg = sqlite3_errmsg(m_sqliteConn);
      sqlite3_close(m_sqliteConn);
      throw exception::Exception(msg);
    }
    {
      char *errMsg = nullptr;
      if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "PRAGMA foreign_keys = ON;", nullptr, nullptr, &errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "Failed to to set PRAGMA foreign_keys = ON";
        if(nullptr != errMsg) {
          ex.getMessage() << ": " << errMsg;
          sqlite3_free(errMsg);
        }
        sqlite3_close(m_sqliteConn);
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

  if(nullptr != m_sqliteConn) {
    sqlite3_close(m_sqliteConn);
    m_sqliteConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<Stmt> SqliteConn::createStmt(const std::string &sql, const Stmt::AutocommitMode autocommitMode) {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    return cta::make_unique<SqliteStmt>(autocommitMode , *this, sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void SqliteConn::commit() {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    if(m_transactionInProgress) {
      char *errMsg = nullptr;
      if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "COMMIT", nullptr, nullptr, &errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "sqlite3_exec failed";
        if(nullptr != errMsg) {
          ex.getMessage() << ": " << errMsg;
          sqlite3_free(errMsg);
        }
        throw ex;
      }
      m_transactionInProgress = false;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void SqliteConn::rollback() {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    if(nullptr == m_sqliteConn) {
      throw exception::Exception("Connection is closed");
    }

    if(m_transactionInProgress) {
      char *errMsg = nullptr;
      if(SQLITE_OK != sqlite3_exec(m_sqliteConn, "ROLLBACK", nullptr, nullptr, &errMsg)) {
        exception::Exception ex;
        ex.getMessage() << "sqlite3_exec failed";
        if(nullptr != errMsg) {
          ex.getMessage() << ": " << errMsg;
          sqlite3_free(errMsg);
        }
        throw ex;
      }
      m_transactionInProgress = false;
    }
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
    auto stmt = createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    os << "NAME, TYPE" << std::endl;
    os << "==========" << std::endl;
    while (rset->next()) {
      const std::string name = rset->columnString("NAME");
      const std::string type = rset->columnString("TYPE");
      os << name << ", " << type << std::endl;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getTableNames() {
  try {
    const char *const sql =
      "SELECT "
        "NAME AS NAME "
      "FROM "
        "SQLITE_MASTER "
      "WHERE "
        "TYPE = 'table' "
      "ORDER BY "
        "NAME;";
    auto stmt = createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    std::list<std::string> names;
    while (rset->next()) {
      names.push_back(rset->columnString("NAME"));
    }
    return names;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool SqliteConn::isOpen() const {
  return nullptr != m_sqliteConn;
}

} // namespace rdbms
} // namespace cta
