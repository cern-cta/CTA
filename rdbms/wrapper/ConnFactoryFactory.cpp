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
//#include "rdbms/wrapper/PostgresConnFactory.hpp"
#include "rdbms/wrapper/SqliteConnFactory.hpp"
#include "plugin-manager/PluginManager.hpp"

#ifdef SUPPORT_OCCI
  #include "rdbms/wrapper/OcciConnFactory.hpp"
#endif

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnFactory> ConnFactoryFactory::create(const Login &login) {
  try {
    static cta::plugin::Manager<rdbms::wrapper::ConnFactory> pm;
    std::unique_ptr<ConnFactory> upConnFactory;

    switch (login.dbType) {
    case Login::DBTYPE_IN_MEMORY:
      return std::make_unique<SqliteConnFactory>("file::memory:?cache=shared");
    case Login::DBTYPE_ORACLE:
#ifdef SUPPORT_OCCI
      return std::make_unique<OcciConnFactory>(login.username, login.password, login.database);
#else
      throw exception::NoSupportedDB("Oracle Catalogue Schema is not supported. Compile CTA with Oracle support.");
#endif
    case Login::DBTYPE_SQLITE:
      return std::make_unique<SqliteConnFactory>(login.database);
    case Login::DBTYPE_POSTGRESQL:
      pm.load("ctardbmspostgres.so")
        .bootstrap("factory");

      upConnFactory = pm.plugin("ctardbmspostgres").create("PostgresConnFactory");
      
      return upConnFactory;

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
