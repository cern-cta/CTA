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
#include "rdbms/wrapper/OcciConn.hpp"
#include "rdbms/wrapper/OcciEnv.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnv::OcciEnv() {
  using namespace oracle::occi;
  m_env = Environment::createEnvironment((oracle::occi::Environment::Mode)(Environment::THREADED_MUTEXED | Environment::OBJECT));
  if(nullptr == m_env) {
    throw exception::Exception(std::string(__FUNCTION__) + "failed"
      ": oracle::occi::createEnvironment() returned a nullptr pointer");
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
// createConn
//------------------------------------------------------------------------------
std::unique_ptr<Conn> OcciEnv::createConn(
  const std::string &username,
  const std::string &password,
  const std::string &database) {
  try {
    oracle::occi::Connection *const conn = m_env->createConnection(username, password, database);
    if (nullptr == conn) {
      throw exception::Exception("oracle::occi::createConnection() returned a nullptr pointer");
    }

    return cta::make_unique<OcciConn>(m_env, conn);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
