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

#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciEnv.hpp"
#include "catalogue/OcciStmt.hpp"
#include "common/exception/Exception.hpp"

#include <stdexcept>
#include <string>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConn::OcciConn(OcciEnv &env, oracle::occi::Connection *const conn):
  m_env(env),
  m_conn(conn) {
  if(NULL == conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed"
      ": The OCCI connection is a NULL pointer");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciConn::~OcciConn() throw() {
  try {
    close(); // Idempotent close() mthod
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciConn::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(m_conn != NULL) {
    m_env->terminateConnection(m_conn);
    m_conn = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Connection *OcciConn::get() const {
  return m_conn;
}

//------------------------------------------------------------------------------
// operator->()
//------------------------------------------------------------------------------
oracle::occi::Connection *OcciConn::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
DbStmt *OcciConn::createStmt(const std::string &sql) {
  try {
    oracle::occi::Statement *const stmt = m_conn->createStatement(sql);
    if (NULL == stmt) {
      throw exception::Exception("oracle::occi::createStatement() returned a NULL pointer");
    }

    return new OcciStmt(sql, *this, stmt);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " +
      ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + sql + ": " + se.what());
  }
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void OcciConn::commit() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void OcciConn::rollback() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

} // namespace catalogue
} // namespace cta
