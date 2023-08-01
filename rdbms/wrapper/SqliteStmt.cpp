/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/CheckConstraintError.hpp"
#include "rdbms/ConstraintError.hpp"
#include "rdbms/PrimaryKeyError.hpp"
#include "rdbms/UniqueConstraintError.hpp"
#include "rdbms/wrapper/Sqlite.hpp"
#include "rdbms/wrapper/SqliteConn.hpp"
#include "rdbms/wrapper/SqliteRset.hpp"
#include "rdbms/wrapper/SqliteStmt.hpp"

#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <unistd.h>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteStmt::SqliteStmt(
  SqliteConn &conn,
  const std::string &sql):
  StmtWrapper(sql),
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
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteStmt::~SqliteStmt() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void SqliteStmt::clear() {
  try {
    threading::MutexLocker locker(m_mutex);

    if(nullptr != m_stmt) {
      const int resetRc = sqlite3_reset(m_stmt);
      if(SQLITE_OK != resetRc) {
        exception::Exception ex;
        ex.getMessage() <<"sqlite3_reset failed: " << Sqlite::rcToStr(resetRc);
      }
      const int clearBindingsRc = sqlite3_clear_bindings(m_stmt);
      if(SQLITE_OK != clearBindingsRc) {
        exception::Exception ex;
        ex.getMessage() <<"sqlite3_clear_bindings failed: " << Sqlite::rcToStr(clearBindingsRc);
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void SqliteStmt::close() {
  try {
    threading::MutexLocker locker(m_mutex);

    if (nullptr != m_stmt) {
      const int finalizeRc = sqlite3_finalize(m_stmt);
      if (SQLITE_OK != finalizeRc) {
        exception::Exception ex;
        ex.getMessage() <<"sqlite3_finalize failed: " << Sqlite::rcToStr(finalizeRc);
      }
      m_stmt = nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
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
// bindUint8
//------------------------------------------------------------------------------
void SqliteStmt::bindUint8(const std::string &paramName, const std::optional<uint8_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_int(m_stmt, paramIdx, paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                               getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindUint16
//------------------------------------------------------------------------------
void SqliteStmt::bindUint16(const std::string &paramName, const std::optional<uint16_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_int(m_stmt, paramIdx, paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                               getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindUint32
//------------------------------------------------------------------------------
void SqliteStmt::bindUint32(const std::string &paramName, const std::optional<uint32_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_int(m_stmt, paramIdx, paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
                               getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void SqliteStmt::bindUint64(const std::string &paramName, const std::optional<uint64_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_int64(m_stmt, paramIdx, (sqlite3_int64) paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindDouble
//------------------------------------------------------------------------------
void SqliteStmt::bindDouble(const std::string &paramName, const std::optional<double> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = 0;
    if(paramValue) {
      bindRc = sqlite3_bind_double(m_stmt, paramIdx, paramValue.value());
    } else {
      bindRc = sqlite3_bind_null(m_stmt, paramIdx);
    }
    if(SQLITE_OK != bindRc) {
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindBlob
//------------------------------------------------------------------------------
void SqliteStmt::bindBlob(const std::string &paramName, const std::string &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName);
    int bindRc = sqlite3_bind_blob(m_stmt, paramIdx, paramValue.c_str(), paramValue.length(), SQLITE_TRANSIENT);
    if(SQLITE_OK != bindRc) {
      exception::Exception ex;
      ex.getMessage() << "sqlite3_bind_blob() failed: " << Sqlite::rcToStr(bindRc);
      throw ex;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindString
//------------------------------------------------------------------------------
void SqliteStmt::bindString(const std::string &paramName, const std::optional<std::string> &paramValue) {
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
      throw exception::Exception(Sqlite::rcToStr(bindRc));
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<RsetWrapper> SqliteStmt::executeQuery() {
  threading::MutexLocker connLocker(m_conn.m_mutex);

  return std::make_unique<SqliteRset>(*this);
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void SqliteStmt::executeNonQuery() {
  threading::MutexLocker connLocker(m_conn.m_mutex);

  const int stepRc = sqlite3_step(m_stmt);

  // Throw an exception if the call to sqlite3_step() failed
  if(SQLITE_DONE != stepRc && SQLITE_ROW != stepRc) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed for SQL statement " << getSqlForException() + ": " << Sqlite::rcToStr(stepRc);

    switch(stepRc) {
    case SQLITE_CONSTRAINT:
      throw ConstraintError(msg.str(), "", "");
    case SQLITE_CONSTRAINT_CHECK:
      throw CheckConstraintError(msg.str(), "", "");
    case SQLITE_CONSTRAINT_PRIMARYKEY:
      throw PrimaryKeyError(msg.str(), "", "");
    case SQLITE_CONSTRAINT_UNIQUE:
      throw UniqueConstraintError(msg.str(), "", "");
    default:
      if ((stepRc & 0xFF) == SQLITE_CONSTRAINT)
        throw ConstraintError(msg.str(), "", "");
      else
        throw exception::Exception(msg.str());
    }
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
// autoCommitModeToBool
//------------------------------------------------------------------------------
bool SqliteStmt::autocommitModeToBool(const AutocommitMode autocommitMode) {
  switch(autocommitMode) {
  case AutocommitMode::AUTOCOMMIT_ON: return true;
  case AutocommitMode::AUTOCOMMIT_OFF: return false;
  default: throw exception::Exception("Unknown autocommit mode");
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
