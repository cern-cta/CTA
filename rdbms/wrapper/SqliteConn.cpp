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
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/Sqlite.hpp"
#include "rdbms/wrapper/SqliteConn.hpp"
#include "rdbms/wrapper/SqliteStmt.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

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

    // Enable extended result codes
    sqlite3_extended_result_codes(m_sqliteConn, 1);

    // Give SQLite upto 120 seconds to avoid a busy error
    sqlite3_busy_timeout(m_sqliteConn, 120000);

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
SqliteConn::~SqliteConn() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteConn::close() {
  threading::MutexLocker locker(m_mutex);

  if(nullptr != m_sqliteConn) {
    const int closeRc = sqlite3_close(m_sqliteConn);
    if(SQLITE_OK != closeRc) {
      exception::Exception ex;
      ex.getMessage() << "Failed to close SQLite connection: " << Sqlite::rcToStr(closeRc);
      throw ex;
    }
    m_sqliteConn = nullptr;
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<Stmt> SqliteConn::createStmt(const std::string &sql, const AutocommitMode autocommitMode) {
  try {
    threading::MutexLocker locker(m_mutex);

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
    threading::MutexLocker locker(m_mutex);

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
    threading::MutexLocker locker(m_mutex);

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
    auto stmt = createStmt(sql, AutocommitMode::AUTOCOMMIT_ON);
    auto rset = stmt->executeQuery();
    os << "NAME, TYPE" << std::endl;
    os << "==========" << std::endl;
    while (rset->next()) {
      const auto name = rset->columnOptionalString("NAME");
      const auto type = rset->columnOptionalString("TYPE");
      os << (name ? name.value() : "NULL") << ", " << (type ? type.value() : "NULL") << std::endl;
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
    auto stmt = createStmt(sql, AutocommitMode::AUTOCOMMIT_ON);
    auto rset = stmt->executeQuery();
    std::list<std::string> names;
    while (rset->next()) {
      auto name = rset->columnOptionalString("NAME");
      if(name) {
        names.push_back(name.value());
      }
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

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> SqliteConn::getSequenceNames() {
  try {
    return std::list<std::string>();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
