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
#include "rdbms/Stmt.hpp"
#include "rdbms/StmtPool.hpp"
#include "rdbms/wrapper/StmtWrapper.hpp"

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
Stmt::Stmt(std::unique_ptr<wrapper::StmtWrapper> stmt, StmtPool &stmtPool):
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
  reset();
}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void Stmt::reset() noexcept {
  try {
    // If this smart prepared statement currently points to a prepared
    // statement then return it back to its pool
    if(nullptr != m_stmtPool && nullptr != m_stmt) {
      m_stmtPool->returnStmt(std::move(m_stmt));
    }
  } catch(...) {
    // Ignore any exceptions
  }

  m_stmtPool = nullptr;
  m_stmt.reset();
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
  try {
    if(nullptr != m_stmt) {
      return m_stmt->getSql();
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// getParamIdx
//-----------------------------------------------------------------------------
uint32_t Stmt::getParamIdx(const std::string &paramName) const {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->getParamIdx(paramName);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindUint8
//-----------------------------------------------------------------------------
void Stmt::bindUint8(const std::string &paramName, const optional<uint8_t> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindUint8(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindUint16
//-----------------------------------------------------------------------------
void Stmt::bindUint16(const std::string &paramName, const optional<uint16_t> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindUint16(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindUint32
//-----------------------------------------------------------------------------
void Stmt::bindUint32(const std::string &paramName, const optional<uint32_t> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindUint32(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindUint64
//-----------------------------------------------------------------------------
void Stmt::bindUint64(const std::string &paramName, const optional<uint64_t> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindUint64(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindDouble
//-----------------------------------------------------------------------------
void Stmt::bindDouble(const std::string &paramName, const optional<double> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindDouble(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindBool
//-----------------------------------------------------------------------------
void Stmt::bindBool(const std::string &paramName, const optional<bool> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindBool(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindBlob
//-----------------------------------------------------------------------------
void Stmt::bindBlob(const std::string &paramName, const std::string &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindBlob(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// bindString
//-----------------------------------------------------------------------------
void Stmt::bindString(const std::string &paramName, const optional<std::string> &paramValue) {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->bindString(paramName, paramValue);
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: paramName=" + paramName + ": " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// executeQuery
//-----------------------------------------------------------------------------
Rset Stmt::executeQuery() {
  try {
    if(nullptr != m_stmt) {
      return Rset(m_stmt->executeQuery());
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// executeNonQuery
//-----------------------------------------------------------------------------
void Stmt::executeNonQuery() {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->executeNonQuery();
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// getNbAffectedRows
//-----------------------------------------------------------------------------
uint64_t Stmt::getNbAffectedRows() const {
  try {
    if(nullptr != m_stmt) {
      return m_stmt->getNbAffectedRows();
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//-----------------------------------------------------------------------------
// getStmt
//-----------------------------------------------------------------------------
wrapper::StmtWrapper &Stmt::getStmt() {
  try {
    if(nullptr != m_stmt) {
      return *m_stmt;
    } else {
      throw exception::Exception("Stmt does not contain a cached statement");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

} // namespace rdbms
} // namespace cta
