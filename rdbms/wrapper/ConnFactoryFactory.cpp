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

#include <string>

#include "common/exception/Exception.hpp"
#include "common/exception/NoSupportedDB.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "plugin-manager/PluginManager.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnFactory> ConnFactoryFactory::create(const Login &login) {
  try {
    static cta::plugin::Manager<rdbms::wrapper::ConnFactory,
      cta::plugin::Args<const std::string&>,
      cta::plugin::Args<const std::string&, const std::string&, const std::string&>> pm;

    switch (login.dbType) {
    case Login::DBTYPE_IN_MEMORY:
    {
      const std::string FILE_NAME = "file::memory:?cache=shared";
      pm.load("libctardbmssqlite.so");
      if (!pm.isRegistered("ctardbmssqlite")) {
        pm.bootstrap("factory");
      }
      return pm.plugin("ctardbmssqlite").make("SqliteConnFactory", FILE_NAME);
    }
    case Login::DBTYPE_ORACLE:
#ifdef SUPPORT_OCCI
      pm.load("libctardbmsocci.so");
      if (!pm.isRegistered("ctardbmsocci")) {
        pm.bootstrap("factory");
      }
      return pm.plugin("ctardbmsocci").make("OcciConnFactory", login.username, login.password, login.database);
#else
      throw exception::NoSupportedDB("Oracle Catalogue Schema is not supported. Compile CTA with Oracle support.");
#endif
    case Login::DBTYPE_SQLITE:
      pm.load("libctardbmssqlite.so");
      if (!pm.isRegistered("ctardbmssqlite")) {
        pm.bootstrap("factory");
      }
      return pm.plugin("ctardbmssqlite").make("SqliteConnFactory", login.database);
    case Login::DBTYPE_POSTGRESQL:
      pm.load("libctardbmspostgres.so");
      if (!pm.isRegistered("ctardbmspostgres")) {
        pm.bootstrap("factory");
      }
      return pm.plugin("ctardbmspostgres").make("PostgresConnFactory", login.database);
    case Login::DBTYPE_NONE:
      throw exception::Exception("Cannot create a catalogue without a database type");
    default:
      {
        exception::Exception ex;
        ex.getMessage() << "Unknown database type: value=" << login.dbType;
        throw ex;
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::rdbms::wrapper
