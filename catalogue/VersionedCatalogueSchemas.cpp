/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/VersionedCatalogueSchemas.hpp"
#include "catalogue/AllCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleVersionedCatalogueSchema::OracleVersionedCatalogueSchema(std::string schemaVersion) {
  try {
    sql = AllCatalogueSchema::mapSchema.at(schemaVersion).at("oracle");
  }
  catch (std::out_of_range&) {
    throw exception::Exception(std::string("No such catalogue version ") + schemaVersion);
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresVersionedCatalogueSchema::PostgresVersionedCatalogueSchema(std::string schemaVersion) {
  try {
    sql = AllCatalogueSchema::mapSchema.at(schemaVersion).at("postgres");
  }
  catch (std::out_of_range&) {
    throw exception::Exception(std::string("No such catalogue version ") + schemaVersion);
  }
}

}  // namespace catalogue
}  // namespace cta
