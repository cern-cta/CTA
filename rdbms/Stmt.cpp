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
#include "rdbms/Stmt.hpp"
#include "rdbms/StmtPool.hpp"
#include "rdbms/wrapper/Stmt.hpp"

namespace cta {
namespace rdbms {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt():
  m_stmtPool(nullptr) {
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt(std::unique_ptr<wrapper::Stmt> stmt, StmtPool &stmtPool):
  m_stmt(std::move(stmt)),
  m_stmtPool(&stmtPool) {
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Stmt::Stmt(Stmt &&other):
  m_stmt(std::move(other.m_stmt)),
  m_stmtPool(other.m_stmtPool){
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
Stmt::~Stmt() noexcept {
  try {
    // If this smart prepared statement currently points to a prepared
    // statement then return it back to its pool
    if(nullptr != m_stmtPool && nullptr != m_stmt) {
      m_stmtPool->returnStmt(std::move(m_stmt));
    }
  } catch(...) {
    // Ignore any exceptions
  }
}

//-----------------------------------------------------------------------------
// operator=
//-----------------------------------------------------------------------------
Stmt &Stmt::operator=(Stmt &&rhs) {
  // If the cached statement is not already owned
  if(rhs.m_stmt != m_stmt) {
    // If this smart cached statement already points to cached statement, then
    // return it back to its pool
    if(nullptr != m_stmt && nullptr != m_stmtPool) {
      m_stmtPool->returnStmt(std::move(m_stmt));
    }

    // Take ownership of the new cached statement
    m_stmt = std::move(rhs.m_stmt);
    m_stmtPool = rhs.m_stmtPool;

    rhs.m_stmtPool = nullptr;
  }

  return *this;
}

//-----------------------------------------------------------------------------
// getSql
//-----------------------------------------------------------------------------
const std::string &Stmt::getSql() const {
  if(nullptr != m_stmt) {
    return m_stmt->getSql();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// getParamIdx
//-----------------------------------------------------------------------------
uint32_t Stmt::getParamIdx(const std::string &paramName) const {
  if(nullptr != m_stmt) {
    return m_stmt->getParamIdx(paramName);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindUint64
//-----------------------------------------------------------------------------
void Stmt::bindUint64(const std::string &paramName, const uint64_t paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindUint64(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindOptionalUint64
//-----------------------------------------------------------------------------
void Stmt::bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindOptionalUint64(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindBool
//-----------------------------------------------------------------------------
void Stmt::bindBool(const std::string &paramName, const bool paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindBool(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindOptionalBool
//-----------------------------------------------------------------------------
void Stmt::bindOptionalBool(const std::string &paramName, const optional<bool> &paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindOptionalBool(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindString
//-----------------------------------------------------------------------------
void Stmt::bindString(const std::string &paramName, const std::string &paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindString(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// bindOptionalString
//-----------------------------------------------------------------------------
void Stmt::bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) {
  if(nullptr != m_stmt) {
    return m_stmt->bindOptionalString(paramName, paramValue);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// executeQuery
//-----------------------------------------------------------------------------
Rset Stmt::executeQuery(const AutocommitMode autocommitMode) {
  if(nullptr != m_stmt) {
    return Rset(m_stmt->executeQuery(autocommitMode));
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// executeNonQuery
//-----------------------------------------------------------------------------
void Stmt::executeNonQuery(const AutocommitMode autocommitMode) {
  if(nullptr != m_stmt) {
    return m_stmt->executeNonQuery(autocommitMode);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// getNbAffectedRows
//-----------------------------------------------------------------------------
uint64_t Stmt::getNbAffectedRows() const {
  if(nullptr != m_stmt) {
    return m_stmt->getNbAffectedRows();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

//-----------------------------------------------------------------------------
// getStmt
//-----------------------------------------------------------------------------
wrapper::Stmt &Stmt::getStmt() {
  if(nullptr != m_stmt) {
    return *m_stmt;
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Stmt does not contain a cached statement");
  }
}

} // namespace rdbms
} // namespace cta
