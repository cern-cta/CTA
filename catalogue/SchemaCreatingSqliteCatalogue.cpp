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

#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"

#include "catalogue/SqliteCatalogueSchema.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SchemaCreatingSqliteCatalogue::SchemaCreatingSqliteCatalogue(log::Logger& log,
                                                             const std::string& filename,
                                                             const uint64_t nbConns,
                                                             const uint64_t nbArchiveFileListingConns)
    : SqliteCatalogue(log, filename, nbConns, nbArchiveFileListingConns) {
  createCatalogueSchema();
}

//------------------------------------------------------------------------------
// createCatalogueSchema
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::createCatalogueSchema() const {
  const SqliteCatalogueSchema schema;
  auto conn = m_connPool->getConn();
  executeNonQueries(conn, schema.sql);
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::executeNonQueries(rdbms::Conn& conn, const std::string& sqlStmts) const {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    // Calculate the length of the current statement without the trailing ';'
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(std::string_view(sqlStmts).substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (0 < sqlStmt.size()) {  // Ignore empty statements
      conn.executeNonQuery(sqlStmt);
    }
  }
}

}  // namespace cta::catalogue
