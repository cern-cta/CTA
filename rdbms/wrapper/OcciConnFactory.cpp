/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "rdbms/wrapper/OcciConnFactory.hpp"
#include "rdbms/wrapper/OcciEnvSingleton.hpp"
#include "plugin-manager/PluginInterface.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConnFactory::OcciConnFactory(const rdbms::Login& login) : m_login(login) {}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> OcciConnFactory::create() {
  try {
    return OcciEnvSingleton::instance().createConn(m_login);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getDbSystemName
//------------------------------------------------------------------------------
std::string OcciConnFactory::getDbSystemName() const {
  return cta::semconv::attr::DbSystemNameValues::kOracle;
}

//------------------------------------------------------------------------------
// getDbNamespace
//------------------------------------------------------------------------------
std::string OcciConnFactory::getDbNamespace() const {
  return m_login.dbNamespace;
}

} // namespace cta::rdbms::wrapper

extern "C" {

void factory(cta::plugin::Interface<cta::rdbms::wrapper::ConnFactory,
                                    cta::plugin::Args<const cta::rdbms::Login&>,
                                    cta::plugin::Args<const cta::rdbms::Login&>>& interface) {
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctardbmsocci")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::rdbms::wrapper::OcciConnFactory>("OcciConnFactory");
}

}// extern "C"
