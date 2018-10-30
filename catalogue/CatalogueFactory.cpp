/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueRetryWrapper.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/OracleCatalogue.hpp"
#include "catalogue/SqliteCatalogue.hpp"
#include "catalogue/MysqlCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> CatalogueFactory::create(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect) {
  try {
    switch(login.dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
      {
        auto c = cta::make_unique<InMemoryCatalogue>(log, nbConns, nbArchiveFileListingConns, maxTriesToConnect);
        return cta::make_unique<CatalogueRetryWrapper>(log, std::move(c), maxTriesToConnect);
      }
    case rdbms::Login::DBTYPE_ORACLE:
      {
        auto c = cta::make_unique<OracleCatalogue>(log, login.username, login.password, login.database, nbConns,
          nbArchiveFileListingConns, maxTriesToConnect);
        return cta::make_unique<CatalogueRetryWrapper>(log, std::move(c), maxTriesToConnect);
      }
    case rdbms::Login::DBTYPE_SQLITE:
      {
        auto c = cta::make_unique<SqliteCatalogue>(log, login.database, nbConns, nbArchiveFileListingConns,
          maxTriesToConnect);
        return cta::make_unique<CatalogueRetryWrapper>(log, std::move(c), maxTriesToConnect);
      }
    case rdbms::Login::DBTYPE_MYSQL:
      {
        auto c = cta::make_unique<MysqlCatalogue>(log, login, nbConns, nbArchiveFileListingConns,
          maxTriesToConnect);
        return cta::make_unique<CatalogueRetryWrapper>(log, std::move(c), maxTriesToConnect);
      }
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

} // namespace catalogue
} // namespace cta
