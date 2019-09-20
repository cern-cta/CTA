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
#include "common/utils/utils.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/rdbms.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn(): m_pool(nullptr) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Conn::Conn(std::unique_ptr<ConnAndStmts> connAndStmts, ConnPool *pool):
  m_connAndStmts(std::move(connAndStmts)),
  m_pool(pool) {
}

//------------------------------------------------------------------------------
// move constructor
//------------------------------------------------------------------------------
Conn::Conn(Conn &&other):
  m_connAndStmts(std::move(other.m_connAndStmts)),
  m_pool(other.m_pool) {
  other.m_pool = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Conn::~Conn() noexcept {
  try {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_connAndStmts) {
      m_pool->returnConn(std::move(m_connAndStmts));
    }
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
Conn &Conn::operator=(Conn &&rhs) {
  // If the database connection is not the one already owned
  if(rhs.m_connAndStmts != m_connAndStmts) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
      m_pool->returnConn(std::move(m_connAndStmts));
    }

    // Take ownership of the new database connection
    m_connAndStmts = std::move(rhs.m_connAndStmts);
    m_pool = rhs.m_pool;

    rhs.m_pool = nullptr;
  }
  return *this;
}

//------------------------------------------------------------------------------
// setAutocommitMode
//------------------------------------------------------------------------------
void Conn::setAutocommitMode(const AutocommitMode autocommitMode) {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->setAutocommitMode(autocommitMode);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getAutocommitMode
//------------------------------------------------------------------------------
AutocommitMode Conn::getAutocommitMode() const {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getAutocommitMode();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
Stmt Conn::createStmt(const std::string &sql) {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->stmtPool->getStmt(*m_connAndStmts->conn, sql);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void Conn::executeNonQuery(const std::string &sql) {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
//  if(std::string::npos != sql.find(";")) {
//    UnexpectedSemicolon ex;
//    ex.getMessage() << "Encountered unexpected semicolon in " << getSqlForException(sql);
//    throw ex;
//  }
    m_connAndStmts->conn->executeNonQuery(sql);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void Conn::commit() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->commit();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void Conn::rollback() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->rollback();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
} 

//------------------------------------------------------------------------------
// getSchemaColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string> Conn::getColumns(const std::string &tableName) const {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getColumns(tableName);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}
  
//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTableNames() const {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTableNames();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getIndexNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getIndexNames() const {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getIndexNames();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// closeUnderlyingStmtsAndConn
//------------------------------------------------------------------------------
void Conn::closeUnderlyingStmtsAndConn() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->stmtPool->clear();
    m_connAndStmts->conn->close();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool Conn::isOpen() const {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->isOpen();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getSequenceNames() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getSequenceNames();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// getTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTriggerNames() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTriggerNames();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Conn does not contain a connection");
  }
}

} // namespace rdbms
} // namespace cta
