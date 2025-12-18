/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciEnv.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NullPtrException.hpp"
#include "rdbms/wrapper/OcciConn.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnv::OcciEnv() {
  using namespace oracle::occi;
  m_env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
  if (nullptr == m_env) {
    throw exception::NullPtrException("oracle::occi::createEnvironment() returned a nullptr");
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
  oracle::occi::Connection* const conn = m_env->createConnection(login.username, login.password, login.database);
  if (nullptr == conn) {
    throw exception::NullPtrException("oracle::occi::createConnection() returned a nullptr");
  }

  return std::make_unique<OcciConn>(m_env, conn, login.dbNamespace);
}

}  // namespace cta::rdbms::wrapper
