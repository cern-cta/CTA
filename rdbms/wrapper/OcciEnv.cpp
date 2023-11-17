/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/OcciConn.hpp"
#include "rdbms/wrapper/OcciEnv.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnv::OcciEnv() {
  using namespace oracle::occi;
  m_env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
  if(nullptr == m_env) {
    throw exception::Exception(std::string(__FUNCTION__) + "failed"
      ": oracle::occi::createEnvironment() returned a nullptr pointer");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciEnv::~OcciEnv() {
  using namespace oracle::occi;

  Environment::terminateEnvironment(m_env);
}

//------------------------------------------------------------------------------
// createConn
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> OcciEnv::createConn(
  const std::string &username,
  const std::string &password,
  const std::string &database) {
  try {
    oracle::occi::Connection *const conn = m_env->createConnection(username, password, database);
    if (nullptr == conn) {
      throw exception::Exception("oracle::occi::createConnection() returned a nullptr pointer");
    }

    return std::make_unique<OcciConn>(m_env, conn);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace cta::rdbms::wrapper
