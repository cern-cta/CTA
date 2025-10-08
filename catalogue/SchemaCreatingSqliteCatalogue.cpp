/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
void SchemaCreatingSqliteCatalogue::createCatalogueSchema() const {
  try {
    const SqliteCatalogueSchema schema;
    auto conn = m_connPool->getConn();
    executeNonQueries(conn, schema.sql);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) const {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(std::string_view(sqlStmts).substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        conn.executeNonQuery(sqlStmt);
      }
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::catalogue
