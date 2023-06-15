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
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/CheckConstraintError.hpp"
#include "rdbms/PrimaryKeyError.hpp"
#include "rdbms/UniqueError.hpp"
#include "rdbms/wrapper/OcciColumn.hpp"
#include "rdbms/wrapper/OcciConn.hpp"
#include "rdbms/wrapper/OcciRset.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

#include <cstring>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciStmt::OcciStmt(const std::string& sql, OcciConn& conn, oracle::occi::Statement* const stmt) :
StmtWrapper(sql),
m_conn(conn),
m_stmt(stmt) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciStmt::~OcciStmt() {
  try {
    close();  // Idempotent close() method
  }
  catch (...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void OcciStmt::clear() {}

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
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               ex.getMessage().str());
  }
  catch (std::exception& se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               se.what());
  }
}

//------------------------------------------------------------------------------
// bindUint8
//------------------------------------------------------------------------------
void OcciStmt::bindUint8(const std::string& paramName, const std::optional<uint8_t>& paramValue) {
  try {
    return bindInteger<uint8_t>(paramName, paramValue);
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint16
//------------------------------------------------------------------------------
void OcciStmt::bindUint16(const std::string& paramName, const std::optional<uint16_t>& paramValue) {
  try {
    return bindInteger<uint16_t>(paramName, paramValue);
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint32
//------------------------------------------------------------------------------
void OcciStmt::bindUint32(const std::string& paramName, const std::optional<uint32_t>& paramValue) {
  try {
    return bindInteger<uint32_t>(paramName, paramValue);
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void OcciStmt::bindUint64(const std::string& paramName, const std::optional<uint64_t>& paramValue) {
  try {
    return bindInteger<uint64_t>(paramName, paramValue);
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// bindBlob
//------------------------------------------------------------------------------
void OcciStmt::bindBlob(const std::string& paramName, const std::string& paramValue) {
  try {
    const unsigned paramIdx = getParamIdx(paramName);
    std::unique_ptr<unsigned char> buffer = std::unique_ptr<unsigned char>(new unsigned char[paramValue.size()]);
    memcpy(buffer.get(), paramValue.c_str(), paramValue.length());
    oracle::occi::Bytes paramBytes(buffer.get(), paramValue.length(), 0);
    m_stmt->setBytes(paramIdx, paramBytes);
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindDouble
//------------------------------------------------------------------------------
void OcciStmt::bindDouble(const std::string& paramName, const std::optional<double>& paramValue) {
  try {
    const unsigned paramIdx = getParamIdx(paramName);
    if (paramValue) {
      // Bind integer as a string in order to support 64-bit integers
      m_stmt->setDouble(paramIdx, paramValue.value());
    }
    else {
      m_stmt->setNull(paramIdx, oracle::occi::OCCIDOUBLE);
    }
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               ex.getMessage().str());
  }
  catch (std::exception& se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               se.what());
  }
}

//------------------------------------------------------------------------------
// bindString
//------------------------------------------------------------------------------
void OcciStmt::bindString(const std::string& paramName, const std::optional<std::string>& paramValue) {
  try {
    if (paramValue && paramValue.value().empty()) {
      throw exception::Exception(
        std::string("Optional string parameter ") + paramName +
        " is an empty string. "
        " An optional string parameter should either have a non-empty string value or no value at all.");
    }

    const unsigned paramIdx = getParamIdx(paramName);
    if (paramValue) {
      m_stmt->setString(paramIdx, paramValue.value());
    }
    else {
      m_stmt->setNull(paramIdx, oracle::occi::OCCISTRING);
    }
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               ex.getMessage().str());
  }
  catch (std::exception& se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + getSqlForException() + ": " +
                               se.what());
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<RsetWrapper> OcciStmt::executeQuery() {
  using namespace oracle;

  const auto autocommitMode = m_conn.getAutocommitMode();

  try {
    switch (autocommitMode) {
      case AutocommitMode::AUTOCOMMIT_ON:
        m_stmt->setAutoCommit(true);
        break;
      case AutocommitMode::AUTOCOMMIT_OFF:
        m_stmt->setAutoCommit(false);
        break;
      default:
        throw exception::Exception("Unknown autocommit mode");
    }

    return std::make_unique<OcciRset>(*this, m_stmt->executeQuery());
  }
  catch (occi::SQLException& ex) {
    std::ostringstream msg;
    msg << std::string(__FUNCTION__) << " failed for SQL statement " << getSqlForException() << ": " << ex.what();

    if (connShouldBeClosed(ex)) {
      // Close the statement first and then the connection
      try {
        close();
      }
      catch (...) {
      }

      try {
        m_conn.close();
      }
      catch (...) {
      }
      throw exception::LostDatabaseConnection(msg.str());
    }
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void OcciStmt::executeNonQuery() {
  using namespace oracle;

  const auto autocommitMode = m_conn.getAutocommitMode();

  try {
    switch (autocommitMode) {
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
  }
  catch (occi::SQLException& ex) {
    std::ostringstream msg;
    msg << std::string(__FUNCTION__) << " failed for SQL statement " << getSqlForException() << ": " << ex.what();

    if (connShouldBeClosed(ex)) {
      // Close the statement first and then the connection
      try {
        close();
      }
      catch (...) {
      }

      try {
        m_conn.close();
      }
      catch (...) {
      }
      throw exception::LostDatabaseConnection(msg.str());
    }

    switch (ex.getErrorCode()) {
      case 1:
        throw UniqueError(msg.str());
      case 2290:
        throw CheckConstraintError(msg.str());
      default:
        throw exception::Exception(msg.str());
    }
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
oracle::occi::Statement* OcciStmt::get() const {
  return m_stmt;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Statement* OcciStmt::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// setColumn
//------------------------------------------------------------------------------
void OcciStmt::setColumn(OcciColumn& col, oracle::occi::Type type) {
  const std::string paramName = std::string(":") + col.getColName();
  const auto paramIdx = getParamIdx(paramName);
  m_stmt->setDataBuffer(paramIdx, col.getBuffer(), type, col.getMaxFieldLength(), col.getFieldLengths());
}

//------------------------------------------------------------------------------
// connShouldBeClosed
//------------------------------------------------------------------------------
bool OcciStmt::connShouldBeClosed(const oracle::occi::SQLException& ex) {
  using namespace oracle;

  switch (ex.getErrorCode()) {
    case 28:
    case 492:
    case 1003:
    case 1008:
    case 1012:
    case 1033:
    case 1089:
    case 2051:
    case 2392:
    case 2396:
    case 2399:
    case 3113:
    case 3114:
    case 3135:
    case 12170:
    case 12514:
    case 12528:
    case 12537:
    case 12541:
    case 12571:
    case 24338:
    case 25401:
    case 25409:
    case 32102:
      return true;
    default:
      return false;
  };
}

}  // namespace wrapper
}  // namespace rdbms
}  // namespace cta
