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

#include <iostream>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PooledConn::PooledConn(Conn *const conn, ConnPool *connPool) noexcept:
  m_connAndPool(conn, connPool) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PooledConn::PooledConn(PooledConn &&other) noexcept:
  m_connAndPool(other.m_connAndPool) {
  other.m_connAndPool = ConnAndPool();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PooledConn::~PooledConn() noexcept {
  try {
    reset();
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void PooledConn::reset(const ConnAndPool &connAndPool) {
  // If the database connection is not the one already owned
  if(connAndPool.conn != m_connAndPool.conn) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(nullptr != m_connAndPool.conn) {
      m_connAndPool.pool->returnConn(m_connAndPool.conn);
    }

    // Take ownership of the new database connection
    m_connAndPool = connAndPool;
  }
}

//------------------------------------------------------------------------------
// get()
//------------------------------------------------------------------------------
Conn *PooledConn::get() const noexcept {
  return m_connAndPool.conn;
}

//------------------------------------------------------------------------------
// operator->()
//------------------------------------------------------------------------------
Conn *PooledConn::operator->() const noexcept {
  return m_connAndPool.conn;
}

//------------------------------------------------------------------------------
// operator*()
//------------------------------------------------------------------------------
Conn &PooledConn::operator*() const {
  if(nullptr == m_connAndPool.conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: No database connection");
  }
  return *m_connAndPool.conn;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
PooledConn &PooledConn::operator=(PooledConn &&rhs) {
  reset(rhs.release());
  return *this;
}

//------------------------------------------------------------------------------
// release
//------------------------------------------------------------------------------
PooledConn::ConnAndPool PooledConn::release() noexcept {
  const ConnAndPool tmp  = m_connAndPool;
  m_connAndPool = ConnAndPool();
  return tmp;
}

} // namespace rdbms
} // namespace cta
