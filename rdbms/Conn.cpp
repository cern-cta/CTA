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
#include "common/utils/utils.cpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Conn.hpp"

namespace cta {
namespace rdbms {

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
// executeNonQueries
//------------------------------------------------------------------------------
void Conn::executeNonQueries(const std::string &sqlStmts, const AutocommitMode autocommitMode) {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        executeNonQuery(sqlStmt, autocommitMode);
      }
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void Conn::executeNonQuery(const std::string &sql, const AutocommitMode autocommitMode) {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    m_connAndStmts->conn->executeNonQuery(sql,autocommitMode);
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
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> Conn::getTableNames() {
  if(nullptr != m_connAndStmts && nullptr != m_connAndStmts->conn) {
    return m_connAndStmts->conn->getTableNames();
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

} // namespace rdbms
} // namespace cta
