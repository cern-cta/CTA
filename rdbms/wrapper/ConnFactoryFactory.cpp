/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/exception/NoSupportedDB.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "plugin-manager/PluginManager.hpp"

#include <string>
#include <sstream>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnFactory> ConnFactoryFactory::create(const Login& login) {
  try {
    static cta::plugin::Manager<rdbms::wrapper::ConnFactory, cta::plugin::Args<const cta::rdbms::Login&>> pm;

    pm.onRegisterPlugin([](const auto& plugin) {
      // API VERSION CHECKING
      if (plugin.template GET<plugin::DATA::API_VERSION>() != VERSION_API) {
        std::ostringstream osErr;
        osErr << "Plugin API version mismatch: "
              << "API_VERSION = " << VERSION_API
              << ", PLUGIN_NAME = " << plugin.template GET<plugin::DATA::PLUGIN_NAME>()
              << ", PLUGIN_API_VERSION = " << plugin.template GET<plugin::DATA::API_VERSION>();
        throw exception::Exception(osErr.str());
      }
    });

    switch (login.dbType) {
      case Login::DBTYPE_IN_MEMORY:
      case Login::DBTYPE_SQLITE:
        pm.load("libctardbmssqlite.so");
        if (!pm.isRegistered("ctardbmssqlite")) {
          pm.bootstrap("factory");
        }
        return pm.plugin("ctardbmssqlite").make("SqliteConnFactory", login);
      case Login::DBTYPE_ORACLE:
#ifdef SUPPORT_OCCI
        pm.load("libctardbmsocci.so");
        if (!pm.isRegistered("ctardbmsocci")) {
          pm.bootstrap("factory");
        }
        return pm.plugin("ctardbmsocci").make("OcciConnFactory", login);
#else
        throw exception::NoSupportedDB("Oracle Catalogue Schema is not supported. Compile CTA with Oracle support.");
#endif
      case Login::DBTYPE_POSTGRESQL:
        pm.load("libctardbmspostgres.so");
        if (!pm.isRegistered("ctardbmspostgres")) {
          pm.bootstrap("factory");
        }
        return pm.plugin("ctardbmspostgres").make("PostgresConnFactory", login);
      case Login::DBTYPE_NONE:
        throw exception::Exception("Cannot create a catalogue without a database type");
      default: {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << login.dbType;
        throw ex;
      }
    }
  } catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

}  // namespace cta::rdbms::wrapper
