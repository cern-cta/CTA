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
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/OcciColumn.hpp"
#include "rdbms/wrapper/OcciConn.hpp"
#include "rdbms/wrapper/OcciRset.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

#include <cstring>
#include <map>
#include <sstream>
#include <stdexcept>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciStmt::OcciStmt(
  const std::string &sql,
  OcciConn &conn,
  oracle::occi::Statement *const stmt) :
  Stmt(sql),
  m_conn(conn),
  m_stmt(stmt) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciStmt::~OcciStmt() {
  try {
    close(); // Idempotent close() method
  } catch (...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void OcciStmt::clear() {
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciStmt::close() {
  try {
    threading::MutexLocker locker(m_mutex);

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
    const unsigned paramIdx = getParamIdx(paramName);
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

    const unsigned paramIdx = getParamIdx(paramName);
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
std::unique_ptr<Rset> OcciStmt::executeQuery(const AutocommitMode autocommitMode) {
  using namespace oracle;

  try {
    switch(autocommitMode) {
    case AutocommitMode::AUTOCOMMIT_ON:
      m_stmt->setAutoCommit(true);
      break;
    case AutocommitMode::AUTOCOMMIT_OFF:
      m_stmt->setAutoCommit(false);
      break;
    default:
     throw exception::Exception("Unknown autocommit mode");
    }

    return cta::make_unique<OcciRset>(*this, m_stmt->executeQuery());
  } catch(occi::SQLException &ex) {
    std::ostringstream msg;
    msg << std::string(__FUNCTION__) << " failed for SQL statement " << getSqlForException() << ": " << ex.what();

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
      throw exception::LostDatabaseConnection(msg.str());
    }
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void OcciStmt::executeNonQuery(const AutocommitMode autocommitMode) {
  using namespace oracle;

  try {
    switch(autocommitMode) {
    case AutocommitMode::AUTOCOMMIT_ON:
      m_stmt->setAutoCommit(true);
      break;
    case AutocommitMode::AUTOCOMMIT_OFF:
      m_stmt->setAutoCommit(false);
      break;
    default:
     throw exception::Exception("Unknown autocommit mode");
    }

    m_stmt->executeUpdate();
  } catch(occi::SQLException &ex) {
    std::ostringstream msg;
    msg << std::string(__FUNCTION__) << " failed for SQL statement " << getSqlForException() << ": " << ex.what();

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
      throw exception::LostDatabaseConnection(msg.str());
    }
    throw exception::Exception(msg.str());
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
// setColumn
//------------------------------------------------------------------------------
void OcciStmt::setColumn(OcciColumn &col) {
  const std::string paramName = std::string(":") + col.getColName();
  const auto paramIdx = getParamIdx(paramName);
  m_stmt->setDataBuffer(paramIdx, col.getBuffer(), oracle::occi::OCCI_SQLT_STR,
    col.getMaxFieldLength(), col.getFieldLengths());
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
  case  2396:
  case  2399:
  case  3113:
  case  3114:
  case  3135:
  case 12170:
  case 12514:
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

} // namespace wrapper
} // namespace rdbms
} // namespace cta
