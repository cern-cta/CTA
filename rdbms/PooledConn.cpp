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
PooledConn::PooledConn(Conn *const conn, ConnPool *pool) noexcept:
  m_conn(conn),
  m_pool(pool) {
}

//------------------------------------------------------------------------------
// move constructor
//------------------------------------------------------------------------------
PooledConn::PooledConn(PooledConn &&other) noexcept:
  m_conn(other.m_conn),
  m_pool(other.m_pool) {
  other.m_conn = nullptr;
  other.m_pool = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PooledConn::~PooledConn() noexcept {
  try {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_conn) {
      m_pool->returnConn(m_conn);
    }
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// get()
//------------------------------------------------------------------------------
Conn *PooledConn::get() const noexcept {
  return m_conn;
}

//------------------------------------------------------------------------------
// operator->()
//------------------------------------------------------------------------------
Conn *PooledConn::operator->() const noexcept {
  return m_conn;
}

//------------------------------------------------------------------------------
// operator*()
//------------------------------------------------------------------------------
Conn &PooledConn::operator*() const {
  if(nullptr == m_conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: No database connection");
  }
  return *m_conn;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
PooledConn &PooledConn::operator=(PooledConn &&rhs) {
  // If the database connection is not the one already owned
  if(rhs.m_conn != m_conn) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_pool && nullptr != m_conn) {
      m_pool->returnConn(m_conn);
    }

    // Take ownership of the new database connection
    m_conn = rhs.m_conn;
    m_pool = rhs.m_pool;

    // Release the database connection from the rhs
    rhs.m_conn = nullptr;
    rhs.m_pool = nullptr;
  }
  return *this;
}

} // namespace rdbms
} // namespace cta
