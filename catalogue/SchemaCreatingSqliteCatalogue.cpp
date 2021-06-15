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

#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SchemaCreatingSqliteCatalogue::SchemaCreatingSqliteCatalogue(
  log::Logger &log,
  const std::string &filename,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  SqliteCatalogue(log, filename, nbConns, nbArchiveFileListingConns) {
  try {
    createCatalogueSchema();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// createCatalogueSchema
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::createCatalogueSchema() {
  try {
    const SqliteCatalogueSchema schema;
    auto conn = m_connPool.getConn();
    executeNonQueries(conn, schema.sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        conn.executeNonQuery(sqlStmt);
      }
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SchemaCreatingSqliteCatalogue::~SchemaCreatingSqliteCatalogue() {
}

} // namespace catalogue
} // namespace cta
