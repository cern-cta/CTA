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

#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/InMemoryCatalogueFactory.hpp"
#include "catalogue/OracleCatalogueFactory.hpp"
#include "catalogue/PostgresqlCatalogueFactory.hpp"
#include "common/make_unique.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<CatalogueFactory> CatalogueFactoryFactory::create(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect) {
  try {
    switch (login.dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
      return cta::make_unique<InMemoryCatalogueFactory>(log, nbConns, nbArchiveFileListingConns, maxTriesToConnect);
    case rdbms::Login::DBTYPE_ORACLE:
      return cta::make_unique<OracleCatalogueFactory>(log, login, nbConns, nbArchiveFileListingConns,
        maxTriesToConnect);
    case rdbms::Login::DBTYPE_POSTGRESQL:
      return cta::make_unique<PostgresqlCatalogueFactory>(log, login, nbConns, nbArchiveFileListingConns, maxTriesToConnect);
    case rdbms::Login::DBTYPE_SQLITE:
      throw exception::Exception("Sqlite file based databases are not supported");
    case rdbms::Login::DBTYPE_NONE:
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

}  // namespace catalogue
}  // namespace cta
