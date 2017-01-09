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
#include "rdbms/OcciConn.hpp"
#include "rdbms/OcciRset.hpp"
#include "rdbms/OcciStmt.hpp"

#include <cstring>
#include <map>
#include <sstream>
#include <stdexcept>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciStmt::OcciStmt(
  const AutocommitMode autocommitMode,
  const std::string &sql,
  OcciConn &conn,
  oracle::occi::Statement *const stmt) :
  Stmt(sql, autocommitMode),
  m_paramNameToIdx(sql),
  m_conn(conn),
  m_stmt(stmt) {

  // m_occiConn and m_stmt have been set and m_stmt is not nullptr so it is safe to
  // call close() from now on
  try {
    switch(autocommitMode) {
    case AutocommitMode::ON:
      m_stmt->setAutoCommit(true);
      break;
    case AutocommitMode::OFF:
      // Do nothing because an occi::Statement has autocommit turned off by default
      break;
    default:
      throw exception::Exception("Unknown autocommit mode");
    }
  } catch(exception::Exception &ex) {
    close();
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  } catch(std::exception &se) {
    close();
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciStmt::~OcciStmt() throw() {
  try {
    close(); // Idempotent close() method
  } catch (...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciStmt::close() {
  try {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (nullptr != m_stmt) {
      m_conn.closeStmt(m_stmt);
      m_stmt = nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void OcciStmt::bindUint64(const std::string &paramName, const uint64_t paramValue) {
  try {
    bindOptionalUint64(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalUint64
//------------------------------------------------------------------------------
void OcciStmt::bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) {
  try {
    const unsigned paramIdx = m_paramNameToIdx.getIdx(paramName);
    if(paramValue) {
      // Bind integer as a string in order to support 64-bit integers
      m_stmt->setString(paramIdx, std::to_string(paramValue.value()));
    } else {
      m_stmt->setNull(paramIdx, oracle::occi::OCCINUMBER);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void OcciStmt::bindString(const std::string &paramName, const std::string &paramValue) {
  try {
    bindOptionalString(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalString
//------------------------------------------------------------------------------
void OcciStmt::bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) {
  try {
    if(paramValue && paramValue.value().empty()) {
      throw exception::Exception(std::string("Optional string parameter ") + paramName + " is an empty string. "
        " An optional string parameter should either have a non-empty string value or no value at all."); 
    }

    const unsigned paramIdx = m_paramNameToIdx.getIdx(paramName);
    if(paramValue) {
      m_stmt->setString(paramIdx, paramValue.value());
    } else {
      m_stmt->setNull(paramIdx, oracle::occi::OCCISTRING);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<Rset> OcciStmt::executeQuery() {
  using namespace oracle;

  try {
    return cta::make_unique<OcciRset>(*this, m_stmt->executeQuery());
  } catch(occi::SQLException &ex) {
    if(connShouldBeClosed(ex)) {
      // Close the statement first and then the connection
      try {
        close();
      } catch(...) {
      }

      try {
        m_conn.close();
      } catch(...) {
      }
    }
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.what());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void OcciStmt::executeNonQuery() {
  using namespace oracle;

  try {
    m_stmt->executeUpdate();
  } catch(occi::SQLException &ex) {
    if(connShouldBeClosed(ex)) {
      // Close the statement first and then the connection
      try {
        close();
      } catch(...) {
      }

      try {
        m_conn.close();
      } catch(...) {
      }
    }
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.what());
  }
}

//------------------------------------------------------------------------------
// getNbAffectedRows
//------------------------------------------------------------------------------
uint64_t OcciStmt::getNbAffectedRows() const {
  return m_stmt->getUb8RowCount();
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::get() const {
  return m_stmt;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// connShouldBeClosed
//------------------------------------------------------------------------------
bool OcciStmt::connShouldBeClosed(const oracle::occi::SQLException &ex) {
  using namespace oracle;

  switch(ex.getErrorCode()) {
  case    28:
  case  1003:
  case  1008:
  case  1012:
  case  1033:
  case  1089:
  case  2392:
  case  2399:
  case  3113:
  case  3114:
  case  3135:
  case 12170:
  case 12541:
  case 12571:
  case 24338:
  case 12537:
  case 25401:
  case 25409:
  case 32102:
    return true;
  default:
    return false;
  };
}

} // namespace rdbms
} // namespace cta
