/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "rdbms/wrapper/SqliteConn.hpp"
#include "rdbms/wrapper/SqliteConnFactory.hpp"
#include "plugin-manager/PluginInterface.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteConnFactory::SqliteConnFactory(const rdbms::Login& login) : m_login(login) {}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> SqliteConnFactory::create() {
  try {
    return std::make_unique<SqliteConn>(m_login);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getDbSystemName
//------------------------------------------------------------------------------
std::string SqliteConnFactory::getDbSystemName() const {
  return cta::semconv::attr::DbSystemNameValues::kSqlite;
}

//------------------------------------------------------------------------------
// getDbNamespace
//------------------------------------------------------------------------------
std::string SqliteConnFactory::getDbNamespace() const {
  return m_login.dbNamespace;
}

} // namespace cta::rdbms::wrapper

extern "C" {

void factory(
  cta::plugin::Interface<cta::rdbms::wrapper::ConnFactory, cta::plugin::Args<const cta::rdbms::Login&>>& interface) {
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctardbmssqlite")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::rdbms::wrapper::SqliteConnFactory>("SqliteConnFactory");
}

}// extern "C"
