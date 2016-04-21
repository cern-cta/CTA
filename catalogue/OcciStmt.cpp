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

// Version 12.1 of oracle instant client uses the pre _GLIBCXX_USE_CXX11_ABI
#define _GLIBCXX_USE_CXX11_ABI 0

#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciStmt.hpp"

#include <cstring>
#include <stdexcept>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciStmt::OcciStmt(const char *const sql, OcciConn &conn, oracle::occi::Statement *const stmt) :
  m_conn(conn),
  m_stmt(stmt) {
  if(NULL == stmt) throw std::runtime_error(std::string(__FUNCTION__) + " failed: stmt is NULL");

  // Work with C strings because they haven't changed with respect to _GLIBCXX_USE_CXX11_ABI
  const std::size_t sqlLen = std::strlen(sql);
  m_sql = new char[sqlLen + 1];
  std::memcpy(m_sql, sql, sqlLen);
  m_sql[sqlLen] = '\0';
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciStmt::~OcciStmt() throw() {
  delete m_sql;

  try {
    close(); // Idempotent close() method
  } catch (...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciStmt::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (NULL != m_stmt) {
    m_conn->terminateStatement(m_stmt);
    m_stmt = NULL;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const char *OcciStmt::getSql() const {
  return m_sql;
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::get() const {
  return m_stmt;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Statement *OcciStmt::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void OcciStmt::bind(const char *paramName, const uint64_t paramValue) {
  std::runtime_error ex(std::string(__FUNCTION__) + " is not implemented");
  throw ex;
}

//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void OcciStmt::bind(const char *paramName, const char *paramValue) {
  std::runtime_error ex(std::string(__FUNCTION__) + " is not implemented");
  throw ex;
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
OcciRset *OcciStmt::execute() {
  return NULL;
}

} // namespace catalogue
} // namespace cta
