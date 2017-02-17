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
#include "rdbms/Sqlite.hpp"
#include "rdbms/SqliteConn.hpp"
#include "rdbms/SqliteRset.hpp"
#include "rdbms/SqliteStmt.hpp"

#include <cstring>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <unistd.h>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteStmt::SqliteStmt(
  const AutocommitMode autocommitMode,
  SqliteConn &conn,
  const std::string &sql):
  Stmt(sql, autocommitMode),
  m_conn(conn),
  m_nbAffectedRows(0) {
  m_stmt = nullptr;
  const int nByte = -1; // Read SQL up to first null terminator

  const uint maxPrepareRetries = 20; // A worst case scenario of 2 seconds
  for(unsigned int i = 1; i <= maxPrepareRetries; i++) {
    const int prepareRc = sqlite3_prepare_v2(m_conn.m_sqliteConn, getSql().c_str(), nByte, &m_stmt, nullptr);

    if(SQLITE_OK == prepareRc) {
      break;
    }

    if(SQLITE_LOCKED == prepareRc) {
      sqlite3_finalize(m_stmt);

      // If the number of retries has not been exceeded
      if(i < maxPrepareRetries) {
        // Sleep for a random length of time less than a tenth of a second
        usleep(random() % 100000);

        // Try to prepare the statement again
        continue;
      } else {
        throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() +
          ": sqlite3_prepare_v2 returned SQLITE_LOCKED the maximum number of " + std::to_string(i) + " times"); 
      }
    }

    const std::string msg = sqlite3_errmsg(m_conn.m_sqliteConn);
    sqlite3_finalize(m_stmt);
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() +
      ": sqlite3_prepare_v2 failed: " + msg);
  }

  // m_stmt has been set so it is safe to call close() from now on
  try {
    switch(autocommitMode) {
    case AutocommitMode::ON:
      // Do nothing because SQLite statements autocommit be default
      break;
    case AutocommitMode::OFF: {
      if(!m_conn.m_transactionInProgress) {
        beginDeferredTransaction();
        m_conn.m_transactionInProgress = true;
      }
      break;
    }
    default:
      throw exception::Exception("Unknown autocommit mode");
    }
  } catch(exception::Exception &ex) {
    close();
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
      ex.getMessage().str());
  } catch(std::exception &se) {
    close();
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
      se.what());
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

  if(nullptr != m_stmt) {
    sqlite3_finalize(m_stmt);
    m_stmt = nullptr;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
sqlite3_stmt *SqliteStmt::get() const {
  if(nullptr == m_stmt) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": nullptr pointer");
  }
  return m_stmt;
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void SqliteStmt::bindUint64(const std::string &paramName, const uint64_t paramValue) {
  try {
    bindOptionalUint64(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalUint64
//------------------------------------------------------------------------------
void SqliteStmt::bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_int64(m_stmt, paramIdx, (sqlite3_int64) paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception("sqlite3_bind_int64() failed");
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindString
//------------------------------------------------------------------------------
void SqliteStmt::bindString(const std::string &paramName, const std::string &paramValue) {
  try {
    bindOptionalString(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalString
//------------------------------------------------------------------------------
void SqliteStmt::bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) {
  try {
    if(paramValue && paramValue.value().empty()) {
      throw exception::Exception(std::string("Optional string parameter ") + paramName + " is an empty string. "
        " An optional string parameter should either have a non-empty string value or no value at all.");
    }
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_text(m_stmt, paramIdx, paramValue.value().c_str(), -1, SQLITE_TRANSIENT);
    } else {    
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception("sqlite3_bind_text() failed");
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str()); 
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<Rset> SqliteStmt::executeQuery() {
  return cta::make_unique<SqliteRset>(*this);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteStmt::executeNonQuery() {
  std::lock_guard<std::mutex> connLock(m_conn.m_mutex);

  const int stepRc = sqlite3_step(m_stmt);

  // Throw an exception if the call to sqlite3_step() failed
  if(SQLITE_DONE != stepRc && SQLITE_ROW != stepRc) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + Sqlite::rcToStr(stepRc));
  }

  m_nbAffectedRows = sqlite3_changes(m_conn.m_sqliteConn);

  // Throw an exception if the SQL statement returned a result set
  if(SQLITE_ROW == stepRc) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": The SQL statment returned a result set");
  }
}

//------------------------------------------------------------------------------
// getNbAffectedRows
//------------------------------------------------------------------------------
uint64_t SqliteStmt::getNbAffectedRows() const {
  return m_nbAffectedRows;
}

//------------------------------------------------------------------------------
// beginDeferredTransaction
//------------------------------------------------------------------------------
void SqliteStmt::beginDeferredTransaction() {
  try {
    char *errMsg = nullptr;
    if(SQLITE_OK != sqlite3_exec(m_conn.m_sqliteConn, "BEGIN DEFERRED", nullptr, nullptr, &errMsg)) {
      exception::Exception ex;
      ex.getMessage() << "sqlite3_exec failed";
      if(nullptr != errMsg) {
        ex.getMessage() << ": " << errMsg;
        sqlite3_free(errMsg);
      }
      throw ex;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace rdbms
} // namespace cta
