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
#include "common/make_unique.hpp"
#include "rdbms/OcciConn.hpp"
#include "rdbms/OcciEnv.hpp"
#include "rdbms/OcciStmt.hpp"

#include <stdexcept>
#include <string>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConn::OcciConn(oracle::occi::Environment *const env, oracle::occi::Connection *const conn):
  m_env(env),
  m_conn(conn) {
  if(nullptr == conn) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed"
      ": The OCCI connection is a nullptr pointer");
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

  if(m_conn != nullptr) {
    m_env->terminateConnection(m_conn);
    m_conn = nullptr;
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
std::unique_ptr<Stmt> OcciConn::createStmt(const std::string &sql) {
  try {
    oracle::occi::Statement *const stmt = m_conn->createStatement(sql);
    if (nullptr == stmt) {
      throw exception::Exception("oracle::occi::createStatement() returned a nullptr pointer");
    }

    return make_unique<OcciStmt>(sql, *this, stmt);
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
  try {
    m_conn->commit();
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void OcciConn::rollback() {
  try {
    m_conn->rollback();
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace rdbms
} // namespace cta
