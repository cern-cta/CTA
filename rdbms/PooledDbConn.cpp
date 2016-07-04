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
#include "rdbms/DbConnPool.hpp"
#include "rdbms/PooledDbConn.hpp"

#include <iostream>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PooledDbConn::PooledDbConn(DbConn *const dbConn, DbConnPool *dbConnPool) noexcept:
  m_dbConnAndPool(dbConn, dbConnPool) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PooledDbConn::PooledDbConn(PooledDbConn &&other) noexcept:
  m_dbConnAndPool(other.m_dbConnAndPool) {
  other.m_dbConnAndPool = DbConnAndPool();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PooledDbConn::~PooledDbConn() noexcept {
  try {
    reset();
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void PooledDbConn::reset(const DbConnAndPool &dbConnAndPool) {
  // If the database connection is not the one already owned
  if(dbConnAndPool.conn != m_dbConnAndPool.conn) {
    // If this smart database connection currently points to a database connection then return it back to its pool
    if(NULL != m_dbConnAndPool.conn) {
      m_dbConnAndPool.pool->returnDbConn(m_dbConnAndPool.conn);
    }

    // Take ownership of the new database connection
    m_dbConnAndPool = dbConnAndPool;
  }
}

//------------------------------------------------------------------------------
// get()
//------------------------------------------------------------------------------
DbConn *PooledDbConn::get() const noexcept {
  return m_dbConnAndPool.conn;
}

//------------------------------------------------------------------------------
// operator->()
//------------------------------------------------------------------------------
DbConn *PooledDbConn::operator->() const noexcept {
  return m_dbConnAndPool.conn;
}

//------------------------------------------------------------------------------
// operator*()
//------------------------------------------------------------------------------
DbConn &PooledDbConn::operator*() const {
  if(NULL == m_dbConnAndPool.conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: No database connection");
  }
  return *m_dbConnAndPool.conn;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
PooledDbConn &PooledDbConn::operator=(PooledDbConn &&rhs) {
  reset(rhs.release());
  return *this;
}

//------------------------------------------------------------------------------
// release
//------------------------------------------------------------------------------
PooledDbConn::DbConnAndPool PooledDbConn::release() noexcept {
  const DbConnAndPool tmp  = m_dbConnAndPool;
  m_dbConnAndPool = DbConnAndPool();
  return tmp;
}

} // namespace rdbms
} // namespace cta
