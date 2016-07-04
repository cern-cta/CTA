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

#include "DbLogin.hpp"
#include "OcciConn.hpp"
#include "OcciEnv.hpp"
#include "common/exception/Exception.hpp"

#include <stdexcept>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnv::OcciEnv() {
  using namespace oracle::occi;
  m_env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
  if(NULL == m_env) {
    throw exception::Exception(std::string(__FUNCTION__) + "failed"
      ": oracle::occi::createEnvironment() returned a NULL pointer");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciEnv::~OcciEnv() throw() {
  using namespace oracle::occi;

  Environment::terminateEnvironment(m_env);
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::Environment *OcciEnv::get() const {
  return m_env;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::Environment *OcciEnv::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// createConn
//------------------------------------------------------------------------------
OcciConn *OcciEnv::createConn(
  const std::string &username,
  const std::string &password,
  const std::string &database) {
  try {
    oracle::occi::Connection *const conn = m_env->createConnection(username, password, database);
    if (NULL == conn) {
      throw exception::Exception("oracle::occi::createConnection() returned a NULL pointer");
    }

    return new OcciConn(*this, conn);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace rdbms
} // namespace cta
