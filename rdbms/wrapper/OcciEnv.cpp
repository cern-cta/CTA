/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
std::unique_ptr<ConnWrapper> OcciEnv::createConn(const rdbms::Login& login) {
  try {
    oracle::occi::Connection* const conn = m_env->createConnection(login.username, login.password, login.database);
    if (nullptr == conn) {
      throw exception::Exception("oracle::occi::createConnection() returned a nullptr pointer");
    }

    return std::make_unique<OcciConn>(m_env, conn, login.dbNamespace);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace cta::rdbms::wrapper
