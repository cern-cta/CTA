/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/PostgresConnFactory.hpp"

#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "plugin-manager/PluginInterface.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"

#include <sstream>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresConnFactory::PostgresConnFactory(const rdbms::Login& login) : m_login(login) {}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> PostgresConnFactory::create() {
  return std::make_unique<PostgresConn>(m_login);
}

//------------------------------------------------------------------------------
// getDbSystemName
//------------------------------------------------------------------------------
std::string PostgresConnFactory::getDbSystemName() const {
  return cta::semconv::attr::DbSystemNameValues::kPostgres;
}

//------------------------------------------------------------------------------
// getDbNamespace
//------------------------------------------------------------------------------
std::string PostgresConnFactory::getDbNamespace() const {
  return m_login.dbNamespace;
}

}  // namespace cta::rdbms::wrapper

extern "C" {

void factory(cta::plugin::Interface<cta::rdbms::wrapper::ConnFactory,
                                    cta::plugin::Args<const cta::rdbms::Login&>,
                                    cta::plugin::Args<const cta::rdbms::Login&>>& interface) {
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctardbmspostgres")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::rdbms::wrapper::PostgresConnFactory>("PostgresConnFactory");
}

}  // extern "C"
