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
#include "rdbms/ConnPool.hpp"
#include "rdbms/PooledConn.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PooledConn::PooledConn(std::unique_ptr<Conn> conn, ConnPool *pool):
  m_conn(conn.release()),
  m_pool(pool) {
}

//------------------------------------------------------------------------------
// move constructor
//------------------------------------------------------------------------------
PooledConn::PooledConn(PooledConn &&other):
  m_conn(std::move(other.m_conn)),
  m_pool(other.m_pool) {
  other.m_pool = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PooledConn::~PooledConn() noexcept {
  try {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_conn) {
      m_pool->returnConn(std::move(m_conn));
    }
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
PooledConn &PooledConn::operator=(PooledConn &&rhs) {
  // If the database connection is not the one already owned
  if(rhs.m_conn != m_conn) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_conn) {
      m_pool->returnConn(std::move(m_conn));
    }

    // Take ownership of the new database connection
    m_conn = std::move(rhs.m_conn);
    m_pool = rhs.m_pool;

    rhs.m_pool = nullptr;
  }
  return *this;
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
std::unique_ptr<Stmt> PooledConn::createStmt(const std::string &sql,
  const Stmt::AutocommitMode autocommitMode) {
  if(nullptr != m_conn) {
    return m_conn->createStmt(sql, autocommitMode);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void PooledConn::executeNonQueries(const std::string &sqlStmts) {
  if(nullptr != m_conn) {
    return m_conn->executeNonQueries(sqlStmts);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void PooledConn::executeNonQuery(const std::string &sql, const Stmt::AutocommitMode autocommitMode) {
  if(nullptr != m_conn) {
    return m_conn->executeNonQuery(sql, autocommitMode);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void PooledConn::commit() {
  if(nullptr != m_conn) {
    m_conn->commit();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void PooledConn::rollback() {
  if(nullptr != m_conn) {
    m_conn->rollback();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
} 

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> PooledConn::getTableNames() {
  if(nullptr != m_conn) {
    return m_conn->getTableNames();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

//------------------------------------------------------------------------------
// isOpen
//------------------------------------------------------------------------------
bool PooledConn::isOpen() const {
  if(nullptr != m_conn) {
    return m_conn->isOpen();
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: PooledConn does not contain a connection");
  }
}

} // namespace rdbms
} // namespace cta
