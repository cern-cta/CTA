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

#include "catalogue/DbLogin.hpp"
#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciEnv.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::OcciEnv::OcciEnv() {
  using namespace oracle::occi;
  m_env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
  if(NULL == m_env) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": oracle::occi::createEnvironment() returned a NULL pointer";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::OcciEnv::~OcciEnv() throw() {
  using namespace oracle::occi;

  Environment::terminateEnvironment(m_env);
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Environment *cta::catalogue::OcciEnv::get() const {
  return m_env;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Environment *cta::catalogue::OcciEnv::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// creatConn
//------------------------------------------------------------------------------
cta::catalogue::OcciConn *cta::catalogue::OcciEnv::createConn(
  const DbLogin &dbLogin) {
  oracle::occi::Connection *const conn = m_env->createConnection(
    dbLogin.username,
    dbLogin.password,
    dbLogin.database);
  if(NULL == conn) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": oracle::occi::createConnection() returned a NULL pointer";
    throw ex;
  }
  return new OcciConn(*this, conn);
}
