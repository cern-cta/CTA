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
#include "catalogue/OcciStmt.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::OcciStmt::OcciStmt(const std::string &sql, OcciConn &conn,
  oracle::occi::Statement *const stmt):
  m_sql(sql),
  m_conn(conn),
  m_stmt(stmt) {
  if(NULL == stmt) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": OCCI statment is a NULL pointer";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::OcciStmt::~OcciStmt() throw() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void cta::catalogue::OcciStmt::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(NULL != m_stmt) {
    m_conn->terminateStatement(m_stmt);
    m_stmt = NULL;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &cta::catalogue::OcciStmt::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Statement *cta::catalogue::OcciStmt::get() const {
  return m_stmt;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Statement *cta::catalogue::OcciStmt::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void cta::catalogue::OcciStmt::bind(const std::string &paramName,
  const uint64_t paramValue) {
  exception::Exception ex;
  ex.getMessage() << __FUNCTION__ << " is not implemented";
  throw ex;
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void cta::catalogue::OcciStmt::bind(const std::string &paramName,
  const std::string &paramValue) {
  exception::Exception ex;
  ex.getMessage() << __FUNCTION__ << " is not implemented";
  throw ex;
}
