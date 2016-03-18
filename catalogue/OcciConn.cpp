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

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::OcciConn::OcciConn(oracle::occi::Environment *const env,
  const DbLogin &dbLogin):
  m_env(env),
  m_conn(m_env->createConnection(
   dbLogin.username,
   dbLogin.password,
   dbLogin.database)) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::OcciConn::~OcciConn() throw() {
  try {
    close(); // Idempotent close() mthod
  } catch(...) {
    // Destructor should not throw any exceptions
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void cta::catalogue::OcciConn::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(m_conn != NULL) {
    m_env->terminateConnection(m_conn);
    m_conn = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Connection *cta::catalogue::OcciConn::get() const {
  return m_conn;
}

//------------------------------------------------------------------------------
// operator->()
//------------------------------------------------------------------------------
oracle::occi::Connection *cta::catalogue::OcciConn::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// createStmt
//------------------------------------------------------------------------------
cta::catalogue::OcciStmt *cta::catalogue::OcciConn::createStmt(
  const std::string &sql) {
  return NULL;
}
